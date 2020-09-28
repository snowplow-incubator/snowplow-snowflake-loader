/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.loader

import cats.{Applicative, ApplicativeError, Apply, MonadError, Show}
import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import cats.effect.{ExitCode, Sync}

import com.snowplowanalytics.snowflake.loader.ast.{Show => StatementShow, _}
import com.snowplowanalytics.snowflake.core.{Config, ProcessManifest, RunId}
import com.snowplowanalytics.snowflake.loader.connection.Database

object Loader {

  /** Common short-circuiting errors happening during loading */
  sealed trait Error extends Product with Serializable

  object Error {
    final case object NothingToLoad extends Error
    final case class PreliminaryCheckFailed(errors: NonEmptyList[String]) extends Error
    final case class WrongFolderPath(folders: NonEmptyList[SnowflakeState.FolderToLoad], actual: Config.S3Folder) extends Error
    final case class ColumnsCorrupted(added: List[String], during: String, error: String, runId: RunId) extends Error
    final case class Runtime(exception: Throwable) extends Error
    final case class CorrruptedManifest(error: String) extends Error

    implicit val loaderErrorShow: Show[Error] =
      Show.show {
        case Error.NothingToLoad =>
          "Nothing to load"
        case Error.PreliminaryCheckFailed(errors) =>
          s"Preliminary checks failed. Loading hasn't started.\n${errors.toList.mkString(", ")}"
        case WrongFolderPath(folders, actual) =>
          s"Following folders are located outside of stage $actual and cannot be loaded\n${folders.map(_.folderToLoad.savedTo)}"
        case Error.ColumnsCorrupted(added, column, error, runId) =>
          val start = s"Error during $column column creation, while loading ${runId.runId}. $error\n"
          val end = if (added.isEmpty)
            "No new columns were added, safe to rerun. "
          else
            s"${added.mkString(", ")} columns were added and probably should be dropped manually. "
          (start ++ end) ++ "Trying to rollback and exit"
        case Runtime(exception) =>
          s"Unhandled runtime exception. ${exception.getMessage}"
        case CorrruptedManifest(error) =>
          s"DynamoDB manifest is corrupted. Manual fix is necessary. $error"
      }
  }

  /** Primary IO action */
  type Action[F[_], A] = EitherT[F, Error, A]

  /**
    * Loader's entry point
    * @param connection DB connection resource, made an argument in order to introspect its state in tests
    * @param config Snowflake JSON configuration
    * @return application exit code based [[Error]]
    */
  def run[F[_]: Sync: Database: ProcessManifest](connection: Database.Connection, config: Config): F[ExitCode] = {
    def initWarehouse: F[Unit] =
      Database[F].execute(connection, UseWarehouse(config.warehouse)) *> {
        Database[F].execute(connection, AlterWarehouse.Resume(config.warehouse)) *>
          Sync[F].delay(println(s"Warehouse ${config.warehouse} resumed"))
      } recoverWith {
        case _: net.snowflake.client.jdbc.SnowflakeSQLException =>
          Sync[F].delay(println(s"Warehouse ${config.warehouse} already resumed"))
      }

    val action: Action[F, Unit] = for {
      _ <- preliminaryChecks(connection, config)
      runIds = ProcessManifest[F].scan(config.manifest)
      state <- SnowflakeState.getState[F](runIds).toAction
      _ <- EitherT.fromEither[F](checkFoldersStage(state.foldersToLoad, config.stageUrl))
      _ <- initWarehouse.toAction
      _ <- state.foldersToLoad.traverse_(loadFolder[F](connection, config))
    } yield ()

    action.value.flatMap {
      case Right(_) =>
        Sync[F].delay(System.out.println("Success. Exiting...")).as(ExitCode.Success)
      case Left(Error.NothingToLoad) =>
        Sync[F].delay(System.out.println("Nothing to load. Exiting...")).as(ExitCode.Success)
      case Left(error) =>
        Sync[F].delay(System.err.println(error.show)).as(ExitCode.Error)
    }
  }

  /**
    * Execute loading statement for processed run id
    * @param connection JDBC connection
    * @param config load configuration
    * @param folder run id, extracted from manifest; processed, but not yet loaded
    */
  def loadFolder[F[_]: Database: ProcessManifest: Sync: ThrowingM]
                (connection: Database.Connection, config: Config)
                (folder: SnowflakeState.FolderToLoad): Action[F, Unit] = {
    val runId = folder.folderToLoad.runIdFolder
    val tempTable = getTempTable(runId, config.schema)
    val transactionName = s"snowplow_${folder.folderToLoad.runIdFolder}".replaceAll("=", "_").replaceAll("-", "_")

    val action: Action[F, Unit] = for {
      loadStatement <- EitherT.fromEither[F](getInsertStatement(config, folder.folderToLoad))
      _ <- Database[F].startTransaction(connection, Some(transactionName)).toAction
      columns <- EitherT(addColumns[F](connection, config.schema, folder))
      _ <- Sync[F].delay { if (columns.isEmpty)
        System.out.println("No new columns added")
      else
        System.out.println(s"Following columns added: ${columns.mkString(", ")}")
      }.toAction
      _ <- loadTempTable[F](connection, config, tempTable, runId).toAction
      _ <- Database[F].execute(connection, loadStatement).toAction
      _ <- Sync[F].delay(println(s"Folder [$runId] from stage [${config.stage}] has been loaded")).toAction
      _ <- ProcessManifest[F].markLoaded(config.manifest, folder.folderToLoad.runId).toAction
      _ <- Database[F].commitTransaction(connection).toAction
    } yield ()

    action.recoverWith { case e => EitherT(Database[F].rollbackTransaction(connection).as(e.asLeft)) }
  }

  /** Check that necessary Snowflake entities are available */
  def preliminaryChecks[F[_]: Database: ThrowingM](connection: Database.Connection, conf: Config): Action[F, Unit] = {
    def exists[S: Statement](statement: S, error: String): F[ValidatedNel[String, Unit]] =
      Database[F].executeAndCountRows(connection, statement).map(_ < 1)
        .ifA(Applicative[F].pure(error.invalidNel[Unit]), Applicative[F].pure(().validNel[String])).attempt.map {
        case Left(e) => s"Preliminary check (${statement.getStatement.value}) failed due SQL exception: ${e.getMessage}".invalidNel
        case Right(original) => original
      }

    val stageUrlCheck: Map[String, Object] => ValidatedNel[String, Unit] = stage =>
      stage.get("url").fold(s"No url info is available for stage [${conf.stage}]".invalidNel[Unit]){ sfStageUrl =>
        if (sfStageUrl == conf.stageUrl.toString) ().validNel[String]
        else (s"Stage [${conf.stage}] is configured to use url [$sfStageUrl] which does not match " +
          s"stageUrl [${conf.stageUrl}] provided in loader configuration").invalidNel[Unit]
      }

    val storageIntegrationCheck: (Map[String, Object], Config.AuthMethod) => ValidatedNel[String, Unit] = (stage, auth) =>
      auth match {
        case Config.AuthMethod.StorageIntegration(intName) =>
          stage.get("storage_integration").fold(s"No storage integration is found for stage [${conf.stage}]".invalidNel[Unit]){ sfIntName =>
            if (sfIntName == intName.toUpperCase) ().validNel[String]
            else (s"Stage [${conf.stage}] is configured to use integration [$sfIntName] which does not match " +
              s"integrationName [$intName] provided in loader configuration").invalidNel[Unit]
          }
        case _ => ().validNel[String]
      }

    val schema = exists(StatementShow.ShowSchemas(Some(conf.schema)), s"Schema ${conf.schema} does not exist")
    val stage = exists(StatementShow.ShowStages(Some(conf.stage), Some(conf.schema)), s"Stage ${conf.stage} does not exist")
    val table = exists(StatementShow.ShowTables(Some(Defaults.Table), Some(conf.schema)), s"Table ${Defaults.Table} does not exist")
    val fileFormat = exists(StatementShow.ShowFileFormats(Some(Defaults.FileFormat), Some(conf.schema)), s"File format ${Defaults.FileFormat} does not exist")
    val warehouse = exists(StatementShow.ShowWarehouses(Some(conf.warehouse)), s"Warehouse ${conf.warehouse} does not exist")
    val stageMatch = Database[F]
      .executeAndReturnResult(connection, StatementShow.ShowStages(Some(conf.stage), Some(conf.schema)))
      .flatMap {
        case Nil => Applicative[F].pure(s"No stages like [${conf.stage}] found in schema [${conf.schema}]".invalidNel[Unit])
        case stage :: Nil => Applicative[F].pure(stageUrlCheck(stage) |+| storageIntegrationCheck(stage, conf.auth))
        case _ => Applicative[F].pure(s"Multiple stages like [${conf.stage}] found in schema [${conf.schema}]".invalidNel[Unit])
      }

    val result = (schema, stage, table, fileFormat, warehouse, stageMatch).tupled.map {
      results => results.tupled.toEither.leftMap(e => Error.PreliminaryCheckFailed(e): Error).void
    }

    EitherT(result)
  }

  /**
    * Create INSERT statement to load Processed Run Id
    * Returns error in
    */
  def getInsertStatement(config: Config, folder: RunId.ProcessedRunId): Either[Error, Insert] =
    getColumns(folder.shredTypes) match {
      case Right(tableColumns) =>
        val castedColumns = tableColumns.map {
          case (name, t @ SnowflakeDatatype.Varchar(Some(size))) if size > 512 =>
            Select.CastedColumn(Defaults.TempTableColumn, name, t, Some(Select.Substring(0, size)))
          case ("refr_term", dataType) =>
            Select.CastedColumn(Defaults.TempTableColumn, "refr_term", dataType, Some(Select.Substring(1, 255)))
          case ("mkt_clickid", dataType) =>
            Select.CastedColumn(Defaults.TempTableColumn, "mkt_clickid", dataType, Some(Select.Substring(1, 128)))
          case (name, dataType) =>
            Select.CastedColumn(Defaults.TempTableColumn, name, dataType)
        }
        val tempTable = getTempTable(folder.runIdFolder, config.schema)
        val source = Select(castedColumns, tempTable.schema, tempTable.name)
        Right(Insert.InsertQuery(config.schema, Defaults.Table, tableColumns.map(_._1), source))
      case Left(error) => Left(Error.CorrruptedManifest(error))
    }

  /**
    * Build CREATE TABLE statement for temporary table
    * @param runId arbitrary identifier
    * @param dbSchema Snowflake DB Schema
    * @return SQL statement AST
    */
  def getTempTable(runId: String, dbSchema: String): CreateTable = {
    val tempTableName = "snowplow_tmp_" + runId.replace('=', '_').replace('-', '_')
    val enrichedColumn = Column(Defaults.TempTableColumn, SnowflakeDatatype.JsonObject, notNull = true)
    CreateTable(dbSchema, tempTableName, List(enrichedColumn), None, temporary = true)
  }

  /**
    * Get list of pairs with column name and datatype based on canonical columns and new shred types
    * @param shredTypes shred types discovered in particular run id
    * @return pairs of string, ready to be used in statement, e.g. (app_id, VARCHAR(255))
    */
  private[loader] def getColumns(shredTypes: List[String]): Either[String, List[(String, SnowflakeDatatype)]] = {
    val atomicColumns = AtomicDef.columns.map(c => (c.name, c.dataType))
    val shredTypeColumns = shredTypes.traverse(c => getShredType(c).toValidatedNel)
    shredTypeColumns match {
      case Validated.Valid(cols) => Right(atomicColumns ++ cols)
      case Validated.Invalid(cols) => Left(s"Columns [${cols.mkString_("", ", ", "")}] are not valid shredded types")
    }
  }

  /**
    * Create temporary table with single OBJECT column and load data from S3
    * @param connection JDBC connection
    * @param config loader configuration
    * @param tempTableCreateStatement SQL statement AST
    * @param runId directory in stage, where files reside
    */
  def loadTempTable[F[_]: Apply: Database](connection: Database.Connection,
                                           config: Config,
                                           tempTableCreateStatement: CreateTable,
                                           runId: String): F[Unit] = {
    val credentials = PasswordService.getLoadCredentials(config.auth) match {
      case Left(PasswordService.NoCredentials) => None
      case Left(PasswordService.CredentialsFailure(error)) => throw new RuntimeException(error)
      case Right(creds) => Some(creds)
    }

    val tempTableCopyStatement = CopyInto(
      tempTableCreateStatement.schema,
      tempTableCreateStatement.name,
      List(Defaults.TempTableColumn),
      CopyInto.From(config.schema, config.stage, runId),
      credentials,
      CopyInto.FileFormat(config.schema, Defaults.FileFormat),
      config.maxError.map(CopyInto.SkipFileNum),
      stripNullValues = true)

    Database[F].execute(connection, tempTableCreateStatement) *>
      Database[F].execute(connection, tempTableCopyStatement)
  }

  /** Try to infer ARRAY/OBJECT type, based on column name */
  def getShredType(columnName: String): Either[String, (String, SnowflakeDatatype)] =
    if (columnName.startsWith("contexts_") && columnName.last.isDigit)
      Right(columnName -> SnowflakeDatatype.JsonArray)
    else if (columnName.startsWith("unstruct_event_") && columnName.last.isDigit)
      Right(columnName -> SnowflakeDatatype.JsonObject)
    else
      Left(columnName)

  /** Check that all folders belong to stage URL (#62) */
  def checkFoldersStage(folders: List[SnowflakeState.FolderToLoad], stage: Config.S3Folder): Either[Error, Unit] =
    NonEmptyList.fromList(folders.filterNot(_.folderToLoad.savedTo.isSubdirOf(stage))) match {
      case None => Right(())
      case Some(folders) => Left(Error.WrongFolderPath(folders, stage): Error)
    }

  /** Add new column with VARIANT type to events table */
  private def addShredType[F[_]: Applicative: Database](connection: Database.Connection,
                                                        schemaName: String,
                                                        columnName: String): F[Either[String, Unit]] =
    getShredType(columnName) match {
      case Right((_, datatype)) =>
        Database[F].execute(connection, AlterTable.AddColumn(schemaName, Defaults.Table, columnName, datatype)) *>
          Applicative[F].pure(().asRight)
      case Left(error) =>
        Applicative[F].pure(error.asLeft)
    }

  /**
    * Add columns for shred types that were encountered for particular folder to events table (in alphabetical order)
    * Print human-readable error and exit in case of error
    * @param connection JDBC connection
    * @param schema Snowflake Schema for events table
    * @param folder folder parsed from manifest
    */
  private def addColumns[F[_]: ThrowingM: Database](connection: Database.Connection,
                                                    schema: String,
                                                    folder: SnowflakeState.FolderToLoad): F[Either[Error, List[String]]] = {
    def go(toAdd: List[String], added: List[String]): F[Either[Error, List[String]]] =
      toAdd match {
        case column :: other => addShredType[F](connection, schema, column).attempt.flatMap {
          case Right(Right(_)) =>
            go(other, column :: added)
          case Right(Left(message)) =>
            Applicative[F].pure(Error.ColumnsCorrupted(added, column, message, folder.folderToLoad).asLeft)
          case Left(dbError) =>
            Applicative[F].pure(Error.ColumnsCorrupted(added, column, dbError.getMessage, folder.folderToLoad).asLeft)
        }
        case Nil => added.asRight[Error].pure[F]
      }

    go(folder.newColumns.toList.sorted, Nil)
  }

  private type Throwing[F[_]] = ApplicativeError[F, Throwable]
  private type ThrowingM[F[_]] = MonadError[F, Throwable]

  private implicit class FOps[F[_]: Throwing, A](value: F[A]) {
    def toAction: Action[F, A] =
      value.attemptT.leftMap(e => Error.Runtime(e): Error)
  }
}
