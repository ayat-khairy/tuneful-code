package cl.cam.ac.uk.tuneful.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;

public class HttpConnector {
	public String connect(String url) {
		HttpResponse response;
		HttpClient httpClient;
		String responseStr = "";
		try {
			httpClient = HttpClients.createDefault();
			HttpGet httpGet = new HttpGet(url);

			response = httpClient.execute(httpGet);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}

			// resturn as a json object
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

			String line;
		
//			System.out.println("Output from Server .... \n");
			while ((line = br.readLine()) != null) {
				responseStr = responseStr + line;

			}

//			System.out.println("RESPONSE >>> " + responseStr);
			
			br.close();

		} catch (MalformedURLException e) {

			e.printStackTrace();

		} catch (IOException e) {

			e.printStackTrace();

		}

		return responseStr;

	}

	public static void main(String[] args) {
		System.out.println( new HttpConnector().connect("http://bda-2.dtg.cl.cam.ac.uk:18080/api/v1/applications/app-20171004102930-0025/stages/168/0/taskList"));
		
		
		//jsonResponse = new JSONArray(responseStr);
	}
}
