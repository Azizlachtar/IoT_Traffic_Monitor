package com.example.iotdashboard.vo;



import com.example.iotdashboard.dao.entity.POITrafficData;
import com.example.iotdashboard.dao.entity.TotalTrafficData;
import com.example.iotdashboard.dao.entity.WindowTrafficData;

import java.io.Serializable;
import java.util.List;

/**
 * Response object containing traffic details that will be sent to dashboard.
 * 
 * @author abaghel
 *
 */
public class Response implements Serializable {
	private List<TotalTrafficData> totalTraffic;
	private List<WindowTrafficData> windowTraffic;
	private List<POITrafficData> poiTraffic;
	
	public List<TotalTrafficData> getTotalTraffic() {
		return totalTraffic;
	}
	public void setTotalTraffic(List<TotalTrafficData> totalTraffic) {
		this.totalTraffic = totalTraffic;
	}
	public List<WindowTrafficData> getWindowTraffic() {
		return windowTraffic;
	}
	public void setWindowTraffic(List<WindowTrafficData> windowTraffic) {
		this.windowTraffic = windowTraffic;
	}
	public List<POITrafficData> getPoiTraffic() {
		return poiTraffic;
	}
	public void setPoiTraffic(List<POITrafficData> poiTraffic) {
		this.poiTraffic = poiTraffic;
	}

}
