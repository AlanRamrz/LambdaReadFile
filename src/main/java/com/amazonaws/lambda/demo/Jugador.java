package com.amazonaws.lambda.demo;
public class Jugador {

	private String clave;
	private String nombre;
	private String posicion;
	
	public Jugador(String clave, String nombre, String posicion) {
		this.clave = clave;
		this.nombre = nombre;
		this.posicion = posicion;
	}

	public String getClave() {
		return clave;
	}

	public void setClave(String clave) {
		this.clave = clave;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getPosicion() {
		return posicion;
	}

	public void setPosicion(String posicion) {
		this.posicion = posicion;
	}

	
	
}
