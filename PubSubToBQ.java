package coreiot;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;

import coreiot.PubSubToBQ;
import coreiot.Read.RowGenerator;
import coreiot.StarterPipeline.StringToRowConverter;

public class PubSubToBQ {
	
	private static final Logger LOG = LoggerFactory.getLogger(PubSubToBQ.class);
	
	 // Cloud Environment Variable Settings
	
	public static String projectId = "gcp-ce-iot-demo";
	public static String subscriptions = "projects/gcp-ce-iot-demo/subscriptions/tour-sub";
	public static String Topic = "projects/gcp-ce-iot-demo/topics/tour-pub";
	public static String datasetId = "sensorDataset";
	public static String tableName = "gcp-ce-iot-demo:sensorDataset.sensorDataTable";
	public static String tableSchema = "temp";
	
	
	 public static class RowGenerator extends DoFn<String, TableRow> implements Serializable {
		    /** Processes an element. */
		    @ProcessElement
		    public void processElement(ProcessContext c) throws Exception {
		      String[] attrs = c.element().split(":");
		      LOG.debug(attrs[0]);
		      TableRow row =
		          new TableRow()
		              .set("temperature", attrs[0]);
		      System.out.print("Row value =>" +row);
		      c.output(row);
		    }
	 }
	
	public static void main(String[] args) {
		
		StreamingOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(StreamingOptions.class);
		options.setStreaming(true);
		
		/* Creating fields */
		System.out.print("Reaching till here...");
		List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
	    fields.add(new TableFieldSchema().setName("deviceid").setType("STRING"));
	    TableSchema schema = new TableSchema().setFields(fields);

	    /* Creating pipe */
		
		Pipeline p = Pipeline.create(options);
		
		p.apply("ReadFromPubsub", PubsubIO.readStrings().fromTopic(Topic))
		
		.apply("Format for Big Query", ParDo.of(new RowGenerator()))
		
		.apply("Write to BigQuery", BigQueryIO.writeTableRows()
				
				.to(tableName)
				.withSchema(schema)
				
				.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(WriteDisposition.WRITE_APPEND));
		
		PipelineResult result = p.run();
		
	}

}
