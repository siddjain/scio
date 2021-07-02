package com.spotify.scio.parquet.avro

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.{
  AddressEvolved,
  ParquetTestRecordEvolved,
  ParquetTestRecordOriginal
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class ParquetAvroSchemaEvolutionTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  def parquetTestRecordEvolved(id: Int): ParquetTestRecordEvolved = {
    ParquetTestRecordEvolved
      .newBuilder()
      .setId(id)
      .setAddress(
        AddressEvolved.newBuilder().setZipcode("someZipcode").setStreet("someStreet").build()
      )
      .build()
  }

  lazy val tempFolder = Files.createTempDirectory("parquetAvroTest")

  lazy val in = tempFolder.resolve("in").toFile

  // Write Parquet-Avro records using an EVOLVED schema
  override def beforeAll(): Unit = {
    val writtenRecords = (1 to 10).map(parquetTestRecordEvolved)

    val sc = ScioContext()
    sc.parallelize(writtenRecords)
      .saveAsParquetAvroFile(in.toString, schema = ParquetTestRecordEvolved.SCHEMA$)
    sc.run()
  }

  override def afterAll(): Unit =
    tempFolder.toFile.delete()

  // Test reading Parquet-Avro records using a projection of the ORIGINAL schema
  "scio-parquet" should "support added fields" in {
    val out = tempFolder.resolve("out1").toFile
    val sc = ScioContext()
    val tap = sc
      .parquetAvroFile[ParquetTestRecordOriginal](
        in.toString + "/*",
        projection = Projection[ParquetTestRecordOriginal](_.getId)
      )
      .map(f => f.getId)
      .saveAsTextFile(out.toString)

    val sr = sc.run().waitUntilDone()
    val openedRecords = tap.get(sr).value.toList
    openedRecords.size shouldBe 10
    openedRecords should contain theSameElementsAs (1 to 10).map(_.toString)
  }

  it should "support reordered fields" in {
    val out = tempFolder.resolve("out2").toFile
    val sc = ScioContext()
    val tap = sc
      .parquetAvroFile[ParquetTestRecordOriginal](
        in.toString + "/*",
        projection = Projection[ParquetTestRecordOriginal](_.getAddress.getZipcode)
      )
      .map(f => f.getAddress.getZipcode)
      .saveAsTextFile(out.toString)

    val sr = sc.run().waitUntilDone()
    val openedRecords = tap.get(sr).value.toList
    openedRecords.size shouldBe 10
    openedRecords.toSet should contain only "someZipcode"
  }
}
