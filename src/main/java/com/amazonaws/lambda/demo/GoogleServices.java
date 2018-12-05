package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.json.JSONObject;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.IOUtils;

public class GoogleServices {
	
	private static final String clientRegion = Region.getRegion(Regions.US_EAST_2).toString();
	private static final String access = "";
	private static final String secret = "";

	public static String downloadImage(String imageName, String clave) {
		String res="";
		
		try{
			imageName = imageName.replaceAll("'", "");
			imageName = imageName.replaceAll(" ", "+");
			
			System.out.println("Vamos a buscar en google: "+imageName);
		
            URL url = new URL("https://www.googleapis.com/customsearch/v1?q="+imageName+"&cx=004963915793980207869:9vgjhg9rjhk&searchType=image&imgType=face&key=AIzaSyCyPXyZR3Hwp3Us8UudmWPJXcqeEmb4Vxo");
            URLConnection connection = url.openConnection();

            String line;
            StringBuilder builder = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            while((line = reader.readLine()) != null) {
                builder.append(line);
            }

            JSONObject json = new JSONObject(builder.toString());
            String imageUrl = json.getJSONArray("items").getJSONObject(0).getString("link");
            
            System.out.println("El url de esa imagen es: "+imageUrl);
            
            saveImageToAws(imageUrl, clave);
            res=getFilesFromBucket(clave);
       
            
        } catch(Exception e){
            e.printStackTrace();
        }
		
		return res;
	}
	
	public static void saveImageToAws(String imageURL, String imageName) throws IOException{
		try {
			BasicAWSCredentials credentials = new BasicAWSCredentials(access, secret);
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(clientRegion).build();
			
			InputStream in = new URL(imageURL).openStream();
			byte[] bytes = IOUtils.toByteArray(in);
			
			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
			
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(bytes.length);
			
			PutObjectRequest request = new PutObjectRequest("lambda-bucket-uia-images", imageName+".jpg", byteArrayInputStream, metadata).withCannedAcl(CannedAccessControlList.PublicRead);
			
			s3Client.putObject(request);
			
			in.close();
			byteArrayInputStream.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static String getFilesFromBucket(String imagen) {
		String res="";
		try {
			BasicAWSCredentials credentials = new BasicAWSCredentials(access, secret);
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(clientRegion).build();
			
			res=s3Client.getUrl("lambda-bucket-uia-images", imagen+".jpg").toExternalForm();
			
			System.out.println("El url del bucket es: "+res);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return res;
	}
}
