package azkaban.common.web;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;

import voldemort.serialization.json.JsonTypeSerializer;
import azkaban.common.web.HdfsSequenceFileViewer;

public class JsonSequenceFileViewer extends HdfsSequenceFileViewer {

    public boolean canReadFile(Reader reader) {
        Text keySchema = reader.getMetadata().get(new Text("key.schema"));
        Text valueSchema = reader.getMetadata().get(new Text("value.schema"));

        return keySchema != null && valueSchema != null;
    }

    public void displaySequenceFile(SequenceFile.Reader reader,
                                    PrintWriter output,
                                    int startLine,
                                    int endLine) throws IOException {
        try {
            BytesWritable keyWritable = new BytesWritable();
            BytesWritable valueWritable = new BytesWritable();
            Text keySchema = reader.getMetadata().get(new Text("key.schema"));
            Text valueSchema = reader.getMetadata().get(new Text("value.schema"));

            JsonTypeSerializer keySerializer = new JsonTypeSerializer(keySchema.toString());
            JsonTypeSerializer valueSerializer = new JsonTypeSerializer(valueSchema.toString());

            // skip lines before the start line
            for(int i = 1; i < startLine; i++)
                reader.next(keyWritable, valueWritable);

            // now actually output lines
            for(int i = startLine; i <= endLine; i++) {
                boolean readSomething = reader.next(keyWritable, valueWritable);
                if(!readSomething)
                    break;
                output.write(keySerializer.toObject(keyWritable.get()).toString());
                output.write("\t=>\t");
                output.write(valueSerializer.toObject(valueWritable.get()).toString());
                output.write("\n");
                output.flush();
            }
        } finally {
            reader.close();
        }
    }
}