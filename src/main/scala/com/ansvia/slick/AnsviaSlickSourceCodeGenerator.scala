package com.ansvia.slick

import java.net.URI

import slick.backend.DatabaseConfig
import slick.codegen.{SourceCodeGenerator, OutputHelpers, AbstractSourceCodeGenerator}
import slick.driver.JdbcProfile
import slick.{model => m}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await}

/**
 * Author: robin
 *
 */


/**
 * My custom slick code generator to fit my taste.
 * @param model Slick data model for which code should be generated.
 */
class AnsviaSlickSourceCodeGenerator(model: m.Model)
    extends AbstractSourceCodeGenerator(model) with OutputHelpers{


    // "Tying the knot": making virtual classes concrete
    type Table = TableDef
    def Table = new TableDef(_)

//    override def code = {
//        "import scala.slick.model.ForeignKeyAction\n" +
//            ( if(tables.exists(_.hlistEnabled)){
//                "import scala.slick.collection.heterogenous._\n"+
//                    "import scala.slick.collection.heterogenous.syntax._\n"
//            } else ""
//                ) +
//            ( if(tables.exists(_.PlainSqlMapper.enabled)){
//                "// NOTE: GetResult mappers for plain SQL are only generated for tables where Slick knows how to map the types of all columns.\n"+
//                    "import scala.slick.jdbc.{GetResult => GR}\n"
//            } else ""
//                ) +
//            "\n/** DDL for all tables. Call .create to execute. */\nlazy val ddl = " + tables.map(t => (jamak(t.TableValue.name.toString)) + ".ddl").mkString(" ++ ") +
//            "\n\n" +
//            tables.map(_.code.mkString("\n")).mkString("\n\n")
//    }

    override def code = {
        "import slick.model.ForeignKeyAction\n" +
            ( if(tables.exists(_.hlistEnabled)){
                "import slick.collection.heterogeneous._\n"+
                    "import slick.collection.heterogeneous.syntax._\n"
            } else ""
                ) +
            ( if(tables.exists(_.PlainSqlMapper.enabled)){
                "// NOTE: GetResult mappers for plain SQL are only generated for tables where Slick knows how to map the types of all columns.\n"+
                    "import slick.jdbc.{GetResult => GR}\n"
            } else ""
                ) +
            (if(ddlEnabled){
                "\n/** DDL for all tables. Call .create to execute. */" +
                    (
                        if(tables.length > 5)
                            "\nlazy val schema: profile.SchemaDescription = Array(" + tables.map(t => jamak(t.TableValue.name.toString) + ".schema").mkString(", ") + ").reduceLeft(_ ++ _)"
                        else if(tables.nonEmpty)
                            "\nlazy val schema: profile.SchemaDescription = " + tables.map(t => jamak(t.TableValue.name.toString) + ".schema").mkString(" ++ ")
                        else
                            "\nlazy val schema: profile.SchemaDescription = profile.DDL(Nil, Nil)"
                        ) +
                    "\n@deprecated(\"Use .schema instead of .ddl\", \"3.0\")"+
                    "\ndef ddl = schema" +
                    "\n\n"
            } else "") +
            tables.map(_.code.mkString("\n")).mkString("\n\n")
    }

    override def entityName: (String) => String = (dbName:String) => dbName.toCamelCase

    protected def jamak(str:String) = if (str.endsWith("s")) str + "es" else str + "s"


    class TableDef(model: m.Table) extends super.TableDef(model){
        // Using defs instead of (caching) lazy vals here to provide consitent interface to the user.
        // Performance should really not be critical in the code generator. Models shouldn't be huge.
        // Also lazy vals don't inherit docs from defs
        type EntityType     =     EntityTypeDef
        def  EntityType     = new EntityType{}
        type PlainSqlMapper =     PlainSqlMapperDef
        def  PlainSqlMapper = new PlainSqlMapper{}
        type TableClass     =     TableClassDef
        def  TableClass     = new TableClass{
            override def code = {
                val prns = parents.map(" with " + _).mkString("")
                val args = model.name.schema.map(n => s"""Some("$n")""") ++ Seq("\""+model.name.table+"\"")
                s"""
class ${name}Row(_tableTag: Tag) extends Table[$elementType](_tableTag, ${args.mkString(", ")})$prns {
  ${indent(body.map(_.mkString("\n")).mkString("\n\n"))}
}
        """.trim()
            }
        }
        type TableValue     =     TableValueDef
        def  TableValue     = new TableValue{
            override def code: String = s"lazy val ${jamak(name.toString)} = new TableQuery(tag => new ${TableClass.name}Row(tag))"
        }
        type Column         =     ColumnDef
        def  Column         = new Column(_)
        type PrimaryKey     =     PrimaryKeyDef
        def  PrimaryKey     = new PrimaryKey(_)
        type ForeignKey     =     MyForeignKeyDef
        def  ForeignKey     = new ForeignKey(_)
        type Index          =     IndexDef
        def  Index          = new Index(_)

        class MyForeignKeyDef(model: m.ForeignKey) extends super.ForeignKeyDef(model){
//            override def code = {
//                val pkTable = jamak(referencedTable.TableValue.name.toString)
//                val (pkColumns, fkColumns) = (referencedColumns, referencingColumns).zipped.map { (p, f) =>
//                    val pk = s"r.${p.name}"
//                    val fk = f.name
//                    if(p.model.nullable && !f.model.nullable) (pk, s"Rep.Some($fk)")
//                    else if(!p.model.nullable && f.model.nullable) (s"Rep.Some($pk)", fk)
//                    else (pk, fk)
//                }.unzip
//                s"""lazy val $name = foreignKey("$dbName", ${compoundValue(fkColumns)}, $pkTable)(r => ${compoundValue(pkColumns)}, onUpdate=${onUpdate}, onDelete=${onDelete})"""
//            }

            override def code = {
                val pkTable = jamak(referencedTable.TableValue.name.toString)
                val (pkColumns, fkColumns) = (referencedColumns, referencingColumns).zipped.map { (p, f) =>
                    val pk = s"r.${p.name}"
                    val fk = f.name
                    if(p.model.nullable && !f.model.nullable) (pk, s"Rep.Some($fk)")
                    else if(!p.model.nullable && f.model.nullable) (s"Rep.Some($pk)", fk)
                    else (pk, fk)
                }.unzip
                s"""lazy val $name = foreignKey("$dbName", ${compoundValue(fkColumns)}, $pkTable)(r => ${compoundValue(pkColumns)}, onUpdate=${onUpdate}, onDelete=${onDelete})"""
            }
        }

    }

}

/** A runnable class to execute the code generator without further setup */
object AnsviaSlickSourceCodeGenerator {

    def run(slickDriver: String, jdbcDriver: String, url: String, outputDir: String, pkg: String, user: Option[String], password: Option[String]): Unit = {
        val driver: JdbcProfile =
            Class.forName(slickDriver + "$").getField("MODULE$").get(null).asInstanceOf[JdbcProfile]
        val dbFactory = driver.api.Database
        val db = dbFactory.forURL(url, driver = jdbcDriver,
            user = user.getOrElse(null), password = password.getOrElse(null), keepAliveConnection = true)
        try {
            val m = Await.result(db.run(driver.createModel(None, true)(ExecutionContext.global).withPinnedSession), Duration.Inf)
            new AnsviaSlickSourceCodeGenerator(m).writeToFile(slickDriver,outputDir,pkg)
        } finally db.close
    }

//    def run(uri: URI, outputDir: Option[String]): Unit = {
//        val dc = DatabaseConfig.forURI[JdbcProfile](uri)
//        val pkg = dc.config.getString("codegen.package")
//        val out = outputDir.getOrElse(dc.config.getString("codegen.outputDir"))
//        val slickDriver = if(dc.driverIsObject) dc.driverName else "new " + dc.driverName
//        try {
//            val m = Await.result(dc.db.run(dc.driver.createModel(None, false)(ExecutionContext.global).withPinnedSession), Duration.Inf)
//            new SourceCodeGenerator(m).writeToFile(slickDriver, out, pkg)
//        } finally dc.db.close
//    }

    def main(args: Array[String]): Unit = {
        args.toList match {
//            case uri :: Nil =>
//                run(new URI(uri), None)
//            case uri :: outputDir :: Nil =>
//                run(new URI(uri), Some(outputDir))
            case slickDriver :: jdbcDriver :: url :: outputDir :: pkg :: Nil =>
                run(slickDriver, jdbcDriver, url, outputDir, pkg, None, None)
            case slickDriver :: jdbcDriver :: url :: outputDir :: pkg :: user :: password :: Nil =>
                run(slickDriver, jdbcDriver, url, outputDir, pkg, Some(user), Some(password))
            case _ => {
                println("""
                          |Usage:
                          |  SourceCodeGenerator configURI [outputDir]
                          |  SourceCodeGenerator slickDriver jdbcDriver url outputDir pkg [user password]
                          |
                          |Options:
                          |  configURI: A URL pointing to a standard database config file (a fragment is
                          |    resolved as a path in the config), or just a fragment used as a path in
                          |    application.conf on the class path
                          |  slickDriver: Fully qualified name of Slick driver class, e.g. "slick.driver.H2Driver"
                          |  jdbcDriver: Fully qualified name of jdbc driver class, e.g. "org.h2.Driver"
                          |  url: JDBC URL, e.g. "jdbc:postgresql://localhost/test"
                          |  outputDir: Place where the package folder structure should be put
                          |  pkg: Scala package the generated code should be places in
                          |  user: database connection user name
                          |  password: database connection password
                          |
                          |When using a config file, in addition to the standard config parameters from
                          |slick.backend.DatabaseConfig you can set "codegen.package" and
                          |"codegen.outputDir". The latter can be overridden on the command line.
                        """.stripMargin.trim)
                System.exit(1)
            }
        }
    }
}
