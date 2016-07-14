package com.ansvia.slick

import slick.codegen.{AbstractSourceCodeGenerator, OutputHelpers}
import slick.driver.JdbcProfile
import slick.model.{ForeignKeyAction, Model}
import slick.{model => m}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}


/**
 * Author: robin
 *
 */


/**
 * My custom slick code generator to fit my taste.
 * @param model Slick data model for which code should be generated.
 */
class AnsviaSlickSourceCodeGenerator(model: m.Model, withEntityExtension:Boolean)
    extends AbstractSourceCodeGenerator(model) with OutputHelpers {


    // "Tying the knot": making virtual classes concrete
    type Table = TableDef
    def Table = { table: m.Table =>
        if (table.columns.size > 22){
            new TableDef(table) with WideTableDef
        }else{
            new TableDef(table)
        }
    }

    protected def customHeadCode = {
        s"""
          |
          |// base entity class
          |trait Entity
          |
          |abstract class BaseTable[T](tag: Tag, name: String) extends Table[T](tag, name) {
          |    val id: Rep[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc)
          |}
        """.stripMargin.trim
    }

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
                ) + customHeadCode +
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
        // Using defs instead of (caching) lazy vals here to provide consistent interface to the user.
        // Performance should really not be critical in the code generator. Models shouldn't be huge.
        // Also lazy vals don't inherit docs from defs
        type EntityType     =     EntityTypeDef
        def  EntityType     = new EntityType{
            override def parents: Seq[String] = {
                if (withEntityExtension){
                    super.parents ++ Seq(name.toString + "Ext", "Entity")
                }else{
                    super.parents ++ Seq("Entity")
                }
            }
        }
        type PlainSqlMapper =     PlainSqlMapperDef
        def  PlainSqlMapper = new PlainSqlMapper{}
        type TableClass     =     TableClassDef
        def  TableClass     = new AnsviaTableClass()

        class AnsviaTableClass extends TableClass {
            override def code = {
                val prns = parents.map(" with " + _).mkString("")
                val args = model.name.schema.map(n => s"""Some("$n")""") ++ Seq("\""+model.name.table+"\"")
                s"""
class ${name}Row(_tableTag: Tag) extends BaseTable[$elementType](_tableTag, ${args.mkString(", ")})$prns {
  ${indent(body.map(_.mkString("\n")).mkString("\n\n"))}
}
        """.trim()
            }
        }
        type TableValue     =     TableValueDef
        def  TableValue     = new TableValue {
            override def code: String = s"lazy val ${jamak(name.toString)} = new TableQuery(tag => new ${TableClass.name}Row(tag))"
//            override def code: String = s"lazy val ${jamak(name.toString)} = new BaseTableQuery[${TableClass.name}, BaseTable[${TableClass.name}]](tag => new ${TableClass.name}Row(tag))".trim()

//            override def code: String = s"lazy val ${jamak(name.toString)} = new TableQuery(tag => new ${TableClass.name}Row(tag)){\n" +
//                "    lazy val findById = this.findBy(t => t.id)\n" +
//                "}\n"
        }
        type Column         =     ColumnDef
//        def  Column         = new Column(_)
        def  Column         = new ColumnDef(_){
//            def defaultCode = {
//                case Some(v) => s"Some(${defaultCode(v)})"
//                case s:String  => "\""+s+"\""
//                case None      => s"None"
//                case v:Byte    => s"$v"
//                case v:Int     => s"$v"
//                case v:Long    => s"${v}L"
//                case v:Float   => s"${v}F"
//                case v:Double  => s"$v"
//                case v:Boolean => s"$v"
//                case v:Short   => s"$v"
//                case v:Char   => s"'$v'"
//                case v:BigDecimal => s"new scala.math.BigDecimal(new java.math.BigDecimal($v))"
//                case v => throw new SlickException( s"Dont' know how to generate code for default value $v of ${v.getClass}. Override def defaultCode to render the value." )
//            }
            // Explicit type to allow overloading existing Slick method names.
            // Explicit type argument for better error message when implicit type mapper not found.
            override def code = {
                (if (name == "id") "override " else "") +
                    s"""val $name: Rep[$actualType] = column[$actualType]("${model.name}"${options.map(", "+_).mkString("")})"""
            }
        }
        type PrimaryKey     =     PrimaryKeyDef
        def  PrimaryKey     = new PrimaryKey(_)
        type ForeignKey     =     MyForeignKeyDef
        def  ForeignKey     = new ForeignKey(_)
        type Index          =     IndexDef
        def  Index          = new Index(_)

        class MyForeignKeyDef(model: m.ForeignKey) extends super.ForeignKeyDef(model){
            override def code = {
                val pkTable = jamak(referencedTable.TableValue.name.toString)
                val (pkColumns, fkColumns) = (referencedColumns, referencingColumns).zipped.map { (p, f) =>
                    val pk = s"r.${p.name}"
                    val fk = f.name.toString

                    if(p.model.nullable && !f.model.nullable) (pk, s"Rep.Some($fk)")
                    else if(!p.model.nullable && f.model.nullable) (s"Rep.Some($pk)", fk)
                    else (pk, fk)
                }.unzip
                s"""lazy val $name = foreignKey("$dbName", ${compoundValue(fkColumns)}, $pkTable)(r => ${compoundValue(pkColumns)}, onUpdate=${onUpdate}, onDelete=${onDelete})"""
            }
        }

    }


    trait WideTableDef {
        self: TableDef =>
        override def hlistEnabled = false

        override def extractor =
            throw new RuntimeException("No extractor is defined for a table with more than 22 columns")

        def compoundRepName = s"${tableName(model.name.table)}Rep"
        def compoundLiftedValue(values: Seq[String]): String = {
            s"""${compoundRepName}(${values.mkString(",")})"""
        }

        override def TableClass = new WideTableClassDef
        class WideTableClassDef extends AnsviaTableClass {
            def repConstructor = compoundLiftedValue(columns.map(_.name))
            override def star = {
                s"def * = ${repConstructor}"
            }
            override def option = {
                s"def ? = Rep.Some(${repConstructor})"
            }
        }

        override def definitions = Seq[Def](
          EntityType, CompoundRep, RowShape, PlainSqlMapper, TableClass, TableValue
        )

        override def compoundValue(values: Seq[String]) = {
//            s"""${entityName(model.name.table)}(${values.mkString(", ")})"""

            if (values.size == 1) values.head
            else {
                s"""(${values.mkString(", ")})"""
            }

        }

//        type PlainSqlMapper =     PlainSqlMapperDef
        override def  PlainSqlMapper = new PlainSqlMapper{
            override def code = {
                val positional = compoundValue(columnsPositional.map(c => (if(c.fakeNullable || c.model.nullable)s"<<?[${c.rawType}]"else s"<<[${c.rawType}]")))
                val dependencies = columns.map(_.exposedType).distinct.zipWithIndex.map{ case (t,i) => s"""e$i: GR[$t]"""}.mkString(", ")
                val rearranged = compoundValue(desiredColumnOrder.map(i => if(hlistEnabled) s"r($i)" else tuple(i)))
                def result(args: String) = if(mappingEnabled) s"$factory($args)" else args
                val body =
                    if(autoIncLastAsOption && columns.size > 1){
                        s"""
val r = $positional
import r._
${result(rearranged)} // putting AutoInc last
            """.trim
                    } else
                        result(s"${entityName(model.name.table)}$positional")
                s"""
implicit def ${name}(implicit $dependencies): GR[${TableClass.elementType}] = GR{
  prs => import prs._
  ${indent(body)}
}
        """.trim
            }
        }

        override def factory = "" //TableClass.elementType

        def CompoundRep = new CompoundRepDef
        class CompoundRepDef extends TypeDef {
            override def code: String = {
                val args = columns.map(c=>
                    c.default.map( v =>
                        s"${c.name}: Rep[${c.exposedType}] = $v"
                    ).getOrElse(
                        s"${c.name}: Rep[${c.exposedType}]"
                    )
                ).mkString(", ")

                val prns = (parents.take(1).map(" extends "+_) ++
                    parents.drop(1).map(" with "+_)).mkString("")

                s"""case class $name($args)$prns"""
            }
            override def doc: String = "" // TODO
            override def rawName: String = compoundRepName
        }

        def RowShape = new RowShapeDef
        class RowShapeDef extends TypeDef {
            override def code: String = {
                val shapes = (columns.map { column =>
                    val repType = s"Rep[${column.exposedType}]"
                    s"implicitly[Shape[FlatShapeLevel, ${repType}, ${column.exposedType}, ${repType}]]"
                }).mkString(", ")
                val shapesSeq = s"Seq(${shapes})"
                def seqConversionCode(columnTypeMapper: String => String) =
                    columns.zipWithIndex.map { case (column, index)=>
                        s"seq(${index}).asInstanceOf[${columnTypeMapper(column.exposedType)}]"
                    }

                val seqParseFunctionBody = seqConversionCode(identity)
                val liftedSeqParseBody = seqConversionCode { tpe => s"Rep[${tpe}]" }
                val seqParseFunction = s"seq => ${entityName(model.name.table)}${compoundValue(seqParseFunctionBody)}"
                val liftedSeqParseFunc = s"seq => ${compoundLiftedValue(liftedSeqParseBody)}"

                s"""implicit object ${name} extends ProductClassShape(${shapesSeq}, ${liftedSeqParseFunc}, ${seqParseFunction})"""

            }
            override def doc: String = "" // TODO
            override def rawName: String = s"${tableName(model.name.table)}Shape"
        }

        class WideIndexDef(index: m.Index) extends self.Index(index) {
            override def code = {
                val unique = if(model.unique) s", unique=true" else ""
                s"""val $name = index("$dbName", ${tuppleValue(columns.map(_.name))}$unique)"""
            }
        }

        override def Index = new WideIndexDef(_)

        def tuppleValue(values: Seq[String]): String = {
            if (values.size == 1) values.head
            else if(values.size <= 22) s"""(${values.mkString(", ")})"""
            else throw new Exception("Cannot generate tuple for > 22 columns.")
        }

    }

}

/** A runnable class to execute the code generator without further setup */
object AnsviaSlickSourceCodeGenerator {

    def run(slickDriver: String, jdbcDriver: String, url: String, outputDir: String, pkg: String, user: Option[String], password: Option[String], withEntityExtension:Boolean): Unit = {
        val driver: JdbcProfile =
            Class.forName(slickDriver + "$").getField("MODULE$").get(null).asInstanceOf[JdbcProfile]
        val dbFactory = driver.api.Database
        val db = dbFactory.forURL(url, driver = jdbcDriver,
            user = user.getOrElse(null), password = password.getOrElse(null), keepAliveConnection = true)
        try {
            val m: Model = Await.result(db.run(driver.createModel(None, true)(ExecutionContext.global).withPinnedSession), Duration.Inf)
            new AnsviaSlickSourceCodeGenerator(m, withEntityExtension).writeToFile(slickDriver,outputDir,pkg)
        } finally db.close
    }

    def main(args: Array[String]): Unit = {
        val withEntityExtension = args.contains("--with-entity-extension")

        args.filterNot(_.startsWith("--")).toList match {
//            case uri :: Nil =>
//                run(new URI(uri), None)
//            case uri :: outputDir :: Nil =>
//                run(new URI(uri), Some(outputDir))
            case slickDriver :: jdbcDriver :: url :: outputDir :: pkg :: Nil =>
                run(slickDriver, jdbcDriver, url, outputDir, pkg, None, None, withEntityExtension)
            case slickDriver :: jdbcDriver :: url :: outputDir :: pkg :: user :: password :: Nil =>
                run(slickDriver, jdbcDriver, url, outputDir, pkg, Some(user), Some(password), withEntityExtension)
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
