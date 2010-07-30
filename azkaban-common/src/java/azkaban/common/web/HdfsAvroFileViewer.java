package azkaban.common.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;




/**
 * This class implements a viewer of avro files
 *  
 * @author lguo
 *
 */
public class HdfsAvroFileViewer implements HdfsFileViewer {

	private static Logger logger = Logger.getLogger(HdfsAvroFileViewer.class);
	
	
	@Override
	public boolean canReadFile(FileSystem fs, Path path) {
				
		if (logger.isDebugEnabled())
			logger.debug("path:" + path.toUri().getPath());
				
		try {
				DataFileStream<Object> avro_datastream = getAvroDataStream(fs, path);
				Schema schema = avro_datastream.getSchema();
				avro_datastream.close();
				return schema != null;
		}catch (IOException e) {
			if (logger.isDebugEnabled()) {
				logger.debug( path.toUri().getPath()  + " is not an avro file.");	
			    logger.debug("Error in getting avro schema: " + e.getLocalizedMessage());
			}
			return false;
		}
	}

	private DataFileStream<Object> getAvroDataStream(FileSystem fs, Path path)
	throws IOException {
		if (logger.isDebugEnabled())
			logger.debug("path:" + path.toUri().getPath());
				
		GenericDatumReader<Object> avro_reader = new GenericDatumReader<Object>();
		InputStream hdfs_inputstream = fs.open(path);
		return new DataFileStream<Object>(hdfs_inputstream, avro_reader);
		
	}
	
	@Override
	public void displayFile(FileSystem fs, Path path, 
			OutputStream outputStream,
			int startLine, int endLine) throws IOException {

		if (logger.isDebugEnabled())
			logger.debug("display avro file:" + path.toUri().getPath());
		
		DataFileStream<Object> avro_datastream = null; 
	    	    
	    try {
	    	avro_datastream = getAvroDataStream(fs, path);
	       	Schema schema = avro_datastream.getSchema();
	    	DatumWriter<Object> avro_writer = new GenericDatumWriter<Object>(schema);
	    	
	    	JsonGenerator g =  new JsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
	    	g.useDefaultPrettyPrinter();	    
	    	Encoder encoder = new JsonEncoder(schema, g);
	    		    	
	    	int lineno = 1; // line number starts from 1
	    	while (avro_datastream.hasNext() && lineno <= endLine) {
	    		Object datum = avro_datastream.next();
	    		if (lineno  >= startLine) {
	    			String record = "\n\n Record " + lineno + ":\n";
	    			outputStream.write(record.getBytes("UTF-8"));
	    			avro_writer.write(datum, encoder);
	    			encoder.flush();
	    		}
	    		lineno ++;
	    	}
		}
	    catch (IOException e) {
	    	outputStream.write( ("Error in display avro file: " + e.getLocalizedMessage()).getBytes("UTF-8"));
	    	throw e;
	    }
	    finally {
	    	avro_datastream.close();	    
	    }
	}


}
