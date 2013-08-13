# Calista is an api to work with the Cassandra.

This works with Cassandra version 0.8. I initially developed this against 0.8.2 but have since
used with 1.1.9 without issue. The basic column and row api, should be backward compatible, but
accessing the column definition will not, and the same applies to cql.

Calista is a Scala based client library for working with a Cassandra datastore and is
designed to be more like working with collections than working with SQL. Calista is built
directly on the low level Thrift api. You can find more information about Cassandra on their
site and wiki here Apache Cassandra.

## Installation

### Fab(ricate)

    dependencies:
      - {conf: compile, org: org.brzy, name: calista, rev: "<current-version>", transitive: false}

### Maven

    <repositories>
      <repository>
        <id>brzy-nexus</id>
        <name>Nexus Staging Repo</name>
        <url>http://repo.brzy.org/content/repositories/releases</url>
      </repository>
    </repositories>
    <dependencies>
        <dependency>
        <groupId>org.brzy</groupId>
        <artifactId>calista</artifactId>
        <version>${calista-version}</version>
      </dependency>
    </dependencies>

## Basic Usage

I'll start off with a small example and explain it after.

    import org.brzy.calista.{SessionManager, Session}
    import org.brzy.calista.schema.StandardColumn

    val sessionManager = new SessionManager()

    sessionManager.doWith { session =>
      val result = StandardColumn("FamilyName")("key")
          .from("columnName")
          .to("otherColumnName")
          .limit(20)
          .reverse.result

      result.rows.foreach(c=>{
        val name = c.nameAs[String]
        val value = c.valueAs[String]
        println( s"$name=$value")
      })
    }

After the imports, you create a new session manager, this is initialized with the defaults
which will connect to localhost on the default port.

The doWith function is a helper function on the manager to wrap the call in a session and
automatically close it. You can manage the session yourself by calling
sessionManager.createSession, then call session.close once finished.

The next line creates a standard column family slice range.
result is a ResultSet object which contains a list or rows. The resultSet has a few helpers to
list the rows. The row itself hold the byte array returned from cassandra, which has to be
converted back to the correct data type.

## Column Types
Calista can interact with any column type with only slightly different syntax.

### Standard Column

    import org.brzy.calista.schema.{StandardColumn=>C}
    val result = C(“family”)(“key”)(“columnName”).valueAs[String]
    C(“family”)(“key”)(“columnName”).set(“newValue”)
    C(“family”)(“key”)(“columnName”).delete

### Super Column

    import org.brzy.calista.schema.{SuperColumn=>C}
    val result = C(“family”)(“key”,”supKey”)(“columnName”).valueAs[String]
    C(“family”)(“key”,”supKey”)(“columnName”).set(“newValue”)
    C(“family”)(“key”,”supKey”)(“columnName”).delete

### Counter Column

    import org.brzy.calista.schema.{CounterColumn=>C}
    val result = C(“family”)(“key”)(“columnName”).count
    C(“family”)(“key”)(“columnName”).add(1)
    C(“family”)(“key”)(“columnName”).delete

### Column Metadata
Column meta data can be read and modified.

    val definition = ColumnFamily(“family”).definition


## Object to Column Mapping (OCM)

This allows you to treat a a Column Family in a similar fashion to how a ORM api maps to a
relational database table. You define the column names that map to a property of a scala object.

A Mapped entity maps to a one Column Family.

    package com.mycompany
    import org.brzy.calista.ocm.{StandardDao,Mapping,Column,Key}

    case class Demo(key:String, name:String)

    object Demo extends StandardDao[Demo]{
      def mapping = Mapping[Demo](
          "Demo", // the column family name
          UTF8Serializer, // the serializer used to read and write the column names
          Key("key"), // key property using the default UTF8 serializer
          Column("name")) // column definition, save to a column named name, using the default UTF8 serializer
    }

With a persistent entity defined in this way, you can now use it like this:

    val newDemo = Demo("key","name").insert
    //...
    val demoFromStore = Demo("key")
    //...
    val demoFromStore2 = Demo.get("key") match {
      case Some(d) => d
      case _ => error("not found")
    }

### Insert

    val demo = new Demo("key","name")
    demo.insert

### Update

    val demo = new Demo("key","name")
    val demo2 = demo.copy(name="bar")
    val demo2.insert

### Delete

    val demo = Demo("key")
    demo.remove

## Common Design Pattern

It's fairly common to store data across multiple column families for a single entity. An
example of this is saving a foreign key relationship or for any other lookup other than the by the
column family key.

    case class Demo(key:String,name:String) extends StandardEntity[String]

    class DemoStore extends StandardDao[UUID,Demo]{

      implicit class DemoCrudOps(d:Demo) extends CrudOps(d) {
        override def insert = {
          val time = System.currentTimeMillis
          StanardColumn("DemoDate")("Date")(time).set(d.key)
          super.insert
        }
      }

      override val mapping = Mapping[Demo](
          "Demo",
          UTF8Serializer,
          Key(“key”),
          Column("name"))

      def listByDate(start:Date,end:Date) =
          StandardColumn("DemoDate")("Date")
            .from(start).to(end).reverse.size(20))
            .map(c=>apply(c.valueAs[String]))
            .toList
    }

Then you can use it like this.

    val before = new Date
    val newOne = Demo("key","john")
    newOne.insert
    val after = new Date
    val listDemosInRange = Demo.listByDate(before,after)
    listDemosInRange.foreach(_.toString)

## Cassandra Query Language.

There is basic support for Cassandra query language assuming the Column Family is setup correctly
in the datastore.

    import org.brzy.calista.schema.Cql
    //...
    sessionManager.doWith { s=>
      val rows = Cql.query("select * from column_family where key='none'")
    }

## Creating Column Family Definitions

Column Families can be created programatically by creating a Column Family Definition and saving
it.

    sessionManager.doWith { s=>
      val famDef = new FamilyDefinition(
        keyspace = "Test",
        name = "create_column_family",
        comparatorType = Option("UTF8Type") ,
        defaultValidationClass = Option("UTF8Type"),
        keyValidationClass = Option("UTF8Type"),
        columnMetadata = Some(List(
          new ColumnDefinition(
            name = "c_name".getBytes(Charset.forName("UTF-8")),
            validationClass = "UTF8Type",
            indexName = None,
            indexType = None
          )
        )).create
      )
    }


## Known Issues, Missing Features

 * No support for authentication.
 * No support for secondary indexes.
 * No support for Hadoop.
 * Partial support for undefined column value data types.

