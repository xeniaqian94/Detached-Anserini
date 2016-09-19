package io.anserini.util;

public class LatLng {
	double lat;
	double lng;

	public LatLng(double lat, double lng) {
		this.lat = lat;
		this.lng = lng;

	}

	public double getLat() {
		return lat;
	}

	public double getLng() {
		return lng;
	}
	
	public boolean withinArea(double thisLat, double thisLng){
		return (Math.abs(thisLat-lat)<0.05) && (Math.abs(thisLng-lng)<0.05);
	}

}
