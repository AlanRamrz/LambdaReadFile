package com.amazonaws.lambda.demo;

import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private final String clientRegion = Region.getRegion(Regions.US_EAST_2).toString();
	private final String access = "";
	private final String secret = "";

    @Override
    public String handleRequest(S3Event event, Context context) {
    	try {
            S3EventNotificationRecord record = event.getRecords().get(0);

            // Retrieve the bucket & key for the uploaded S3 object that
            // caused this Lambda function to be triggered
            String bkt = record.getS3().getBucket().getName();
            String key = record.getS3().getObject().getKey().replace('+', ' ');
            key = URLDecoder.decode(key, "UTF-8");

            // Read the source file as text
            BasicAWSCredentials credentials = new BasicAWSCredentials(access, secret);
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(clientRegion).build();
			
            String body = s3Client.getObjectAsString(bkt, key);
            body = body.replaceAll("\"", "");
            body = body.replaceAll("'", "");
            
            String[] arreglo = body.split(",");
    		String fechaPartido = arreglo[0];
    		String equipoVisitante = arreglo[3];
    		String equipoLocal = arreglo[6];
    		String scoreVisitante = arreglo[9];
    		String scoreLocal = arreglo[10];
    		String[] arregloEquipoVisitante = Arrays.copyOfRange(arreglo, 105, 132);
    		String[] arregloEquipoLocal = Arrays.copyOfRange(arreglo, 132, 159);
    		
    		List<Jugador> listaLocal = obtenerListaEquipo(arregloEquipoLocal);
    		List<Jugador> listaVisitante = obtenerListaEquipo(arregloEquipoVisitante);
    		
    		tryInsertToDb(fechaPartido, equipoLocal, equipoVisitante, scoreLocal, scoreVisitante, listaLocal, listaVisitante);
            
            return "ok";
        } 
    	catch (Exception e) {
            System.err.println("Exception: " + e);
            return "error";
        }

    }
    
    public List<Jugador> obtenerListaEquipo(String[] arreglo) {
		List<Jugador> res = new ArrayList<>();
		int size = arreglo.length;
		int i = 0;

		while (i < size) {
			res.add(new Jugador(arreglo[i], arreglo[i + 1], arreglo[i + 2]));
			i += 3;
		}

		return res;
	}
	
	public void tryInsertToDb(String fecha, String local, String visitante, String scoreLocal, String scoreVisitante, List<Jugador> listaLocal, List<Jugador> listaVisita) {
		
		if(!existePartido(fecha, local, visitante)) {
			System.out.println("El partido no existe...vamos a insertar el partido.");
			insertarPartido(fecha, local, visitante, scoreLocal, scoreVisitante);
			
			System.out.println("Vamos a insertar la relacion entre los jugadores y el partido.");
			insertarPartidoJugador(fecha, local, visitante, listaLocal, listaVisita);
			
			System.out.println("Vamos a ver si ya tenemos a los jugadores en la base de datos.");
			tryInsertPlayers(listaLocal);
			tryInsertPlayers(listaVisita);
		}
		else {
			System.out.println("El partido si existe, no se inserta nada.");
		}
	}
	
	public boolean existePartido(String fecha, String equipoLocal, String equipoVisitante) {
		boolean res=true;
		String generarClave=fecha+"-"+equipoLocal+"-"+equipoVisitante;
		//String generarClave="20170402-caz-ame";
		Connection conn = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://0.0.0.0:3306/mlb", "aramirez", "Passw0rd");
			Statement stmt = conn.createStatement();
			String query = "select * from partidos where ID='"+generarClave+"';";
			ResultSet rs = stmt.executeQuery(query);
			
			if(rs.next()) {
				res=true;
			}
			else {
				res=false;
			}

			conn.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return res;
	}
	
	public void insertarPartido(String fecha, String equipoLocal, String equipoVisitante, String scoreLocal, String scoreVisita) {
		
		String generarClave=fecha+"-"+equipoLocal+"-"+equipoVisitante;
		String generarMarcador = scoreLocal+" - "+scoreVisita;

		Connection conn = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://0.0.0.0:3306/mlb", "aramirez", "Passw0rd");
			Statement stmt = conn.createStatement();
			String query = "insert into partidos values('"+generarClave+"','"+fecha+"','"+equipoLocal+"','"+equipoVisitante+"','"+generarMarcador+"');";
			stmt.executeUpdate(query);
			
			conn.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void insertarPartidoJugador(String fecha, String local, String visitante, List<Jugador> listaLocal, List<Jugador> listaVisita) {
		String generarClave=fecha+"-"+local+"-"+visitante;
		Connection conn = null;
		String query = "";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://0.0.0.0:3306/mlb", "aramirez", "Passw0rd");
			Statement stmt = conn.createStatement();
			
			for(Jugador item : listaLocal) {
				query="insert into partido_jugador values(0,'"+generarClave+"','"+item.getClave()+"','"+item.getPosicion()+"','Local');";
				stmt.executeUpdate(query);
			}
			
			for(Jugador item : listaVisita) {
				query="insert into partido_jugador values(0,'"+generarClave+"','"+item.getClave()+"','"+item.getPosicion()+"','Visitante');";
				stmt.executeUpdate(query);
			}
			
			conn.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void tryInsertPlayers(List<Jugador> lista) {
		Connection conn = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://0.0.0.0:3306/mlb", "aramirez", "Passw0rd");
			Statement stmt = conn.createStatement();
			
			for(Jugador item : lista) {
				
				if(!existeJugador(item.getClave(), stmt)) {
					System.out.println("Este jugador no existe, entonces lo insertamos");
					insertarJugador(item.getClave(), item.getNombre(), stmt);
				}
				else {
					System.out.println("Este jugador ya existe.");
				}
			}
			
			conn.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public boolean existeJugador(String clave, Statement stmt) {
		boolean res=true;
		
		try {
			String query="select * from jugadores where Clave='"+clave+"';";
			ResultSet rs = stmt.executeQuery(query);
			
			if(rs.next()) {
				res=true;
			}
			else {
				res=false;
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		
		
		return res;
	}
	
	public void insertarJugador(String clave, String nombre, Statement stmt) {
		
		try {
			String url = GoogleServices.downloadImage(nombre, clave);
			//String url="";
			String query="insert into jugadores values('"+clave+"','"+nombre+"','"+url+"');";
			stmt.executeUpdate(query);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
    
    
}