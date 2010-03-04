package azkaban.jobcontrol.impl.helper;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

public class StreamGobbler extends Thread {

    private final InputStream _inputStream;
    private final OutputStream _redirect;

    private static final Logger log = Logger.getLogger(StreamGobbler.class.getName());

    public StreamGobbler(InputStream inputStream, OutputStream redirect) {
        _inputStream = inputStream;
        _redirect = redirect;
    }

    public void run() {
        try {
            BufferedInputStream bufReader = new BufferedInputStream(_inputStream);
            BufferedOutputStream bufWriter = new BufferedOutputStream(_redirect);

            while(true) {

                if(-1 == readAndWrite(bufReader, bufWriter)) {
                    break;
                }

                try {
                    Thread.sleep(10);
                } catch(InterruptedException e) {
                    readAndWrite(bufReader, bufWriter);
                    break;
                }
            }

            // close the buffers
            bufReader.close();
            bufWriter.close();
            _inputStream.close();
            _redirect.close();

        } catch(IOException ioe) {
            log.info("Exception reading/Writing from Process buffers", ioe);
        }
    }

    private int readAndWrite(InputStream bufReader, OutputStream bufWriter) throws IOException {
        byte[] buffer = new byte[Math.max(1, bufReader.available())];
        int read = bufReader.read(buffer);

        if(-1 == read) {
            return -1;
        }

        bufWriter.write(buffer, 0, read);
        bufWriter.flush();

        return read;
    }

}
