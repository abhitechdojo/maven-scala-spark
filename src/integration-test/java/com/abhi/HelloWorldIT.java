package com.abhi;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import static org.apache.avro.file.DataFileReader.openReader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

public class HelloWorldIT {

    @Test
    public void lookForSuccessFlag() throws IOException, ParseException {
        String urlStr = System.getProperty("webhdfsurl") + "/webhdfs/v1/output?op=LISTSTATUS";
        String output = getJSONResponse(urlStr);
        JSONArray files = getFileStatusArray(output);
        boolean found = getSuccessFlag(files);
        assertTrue(found);
    }

    @Test
    public void checkRecordCount() throws MalformedURLException, IOException, InterruptedException, ParseException {
        // make a rest call to webhdfs to get list of files
        String fileUrlStr = System.getProperty("webhdfsurl") + "/webhdfs/v1/output/part-00000.avro?op=OPEN";
        String fileUrl = getFileUrl(fileUrlStr);
        assertNotNull(fileUrl);
        String count = getRecordCount(fileUrl);
        assertEquals(Integer.parseInt(count), 2);
    }

    private JSONArray getFileStatusArray(String output) throws ParseException {
        JSONParser parser = new JSONParser();
        JSONObject root = (JSONObject) parser.parse(output);
        JSONObject fileStatusesNode = (JSONObject) root.get("FileStatuses");
        assertNotNull(fileStatusesNode);
        return (JSONArray) fileStatusesNode.get("FileStatus");
    }

    private String getRecordCount(String fileUrl) throws IOException, ParseException {
        URL url = new URL(fileUrl);
        URLConnection urlCon = url.openConnection();
        InputStream input = urlCon.getInputStream();
        byte[] buffer = new byte[4096];
        int n = - 1;
        File f = new File("./part-00000.avro");
        f.deleteOnExit();
        OutputStream os = new FileOutputStream(f);
        while ( (n = input.read(buffer)) != -1)
        {
            os.write(buffer, 0, n);
        }
        os.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(f, reader);

        GenericRecord record = null;
        while (fileReader.hasNext()) {
            record = fileReader.next(record);
        }
        fileReader.close();
        String content = record.toString();
        JSONParser parser = new JSONParser();
        JSONObject obj = (JSONObject) parser.parse(content);
        return (String) obj.get("count");
    }

    private String getFileUrl(String fileUrlStr) throws IOException {
        URL url = new URL(fileUrlStr);
        URLConnection urlCon = url.openConnection();
        HttpURLConnection httpCon = (HttpURLConnection) urlCon;
        httpCon.setInstanceFollowRedirects(false);
        String nextUrl = urlCon.getHeaderField("Location");
        System.out.println(nextUrl);
        Pattern p = Pattern.compile("^http://([^:]*):50075(.*)$");
        Matcher m = p.matcher(nextUrl);
        String fileUrl = null;
        if (m.matches()) {
            String remaining = m.group(2);
            fileUrl = System.getProperty("datanodeurl") + remaining;
        }
        return fileUrl;
    }

    // java 8 ???
    private boolean getSuccessFlag(JSONArray files) {
        boolean found = false;
        Iterator<JSONObject> fileIter = files.iterator();
        while(fileIter.hasNext()) {
            JSONObject file = (JSONObject) fileIter.next();
            String pathSuffix = (String) file.get("pathSuffix");
            if (pathSuffix.equals("_SUCCESS")) {
                found = true;
                break;
            }
        }
        return found;
    }

    private String getJSONResponse(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        URLConnection urlCon = url.openConnection();
        // read the data from the web service
        BufferedReader br = new BufferedReader(new InputStreamReader(urlCon.getInputStream()));
        String l = null;
        String output = "";
        while ((l=br.readLine())!=null) {
            output += l;
        }
        br.close();
        return output;
    }
}