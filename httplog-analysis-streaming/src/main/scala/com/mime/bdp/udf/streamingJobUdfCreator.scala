package com.mime.bdp.udf

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object streamingJobUdfCreator extends AbstractUdfCreator {

  def createAvroTypeUdf(schemaString: String): UserDefinedFunction = {
    require(StringUtils.isNotBlank(schemaString), "schema should not be none!")

    lazy val decoderFactory: DecoderFactory = DecoderFactory.get()
    lazy val schema: Schema = new Schema.Parser().parse(schemaString)
    lazy val reader: DatumReader[GenericRecord] = new GenericDatumReader(schema)

    val deserialize: (Array[Byte]) => String = data => {
      val decoder = decoderFactory.binaryDecoder(data, null)
      reader.read(null, decoder).toString
    }

    udf(deserialize)
  }

}
