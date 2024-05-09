package com.example.iotdashboard.dashboard;


import com.example.iotdashboard.dao.POITrafficDataRepository;
import com.example.iotdashboard.dao.TotalTrafficDataRepository;
import com.example.iotdashboard.dao.WindowTrafficDataRepository;
import com.example.iotdashboard.dao.entity.POITrafficData;
import com.example.iotdashboard.dao.entity.TotalTrafficData;
import com.example.iotdashboard.dao.entity.WindowTrafficData;
import com.example.iotdashboard.vo.Response;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Service class to send traffic data messages to dashboard ui at fixed interval using web-socket.
 * 
 * @author abaghel
 *
 */
@Service
public class TrafficDataService {
	private static final Logger logger = Logger.getLogger(TrafficDataService.class);
	
	@Autowired
	private SimpMessagingTemplate template;
	
	@Autowired
	private TotalTrafficDataRepository totalRepository;
	
	@Autowired
	private WindowTrafficDataRepository windowRepository;
	
	@Autowired
	private POITrafficDataRepository poiRepository;
	
	private static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	//Method sends traffic data message in every 5 seconds.
	@Scheduled(fixedRate = 5000)
	public void trigger() {
		List<TotalTrafficData> totalTrafficList = new ArrayList<TotalTrafficData>();
		List<WindowTrafficData> windowTrafficList = new ArrayList<WindowTrafficData>();
		List<POITrafficData> poiTrafficList = new ArrayList<POITrafficData>();
		//Call dao methods
		totalRepository.findTrafficDataByDate(sdf.format(new Date())).forEach(e -> totalTrafficList.add(e));	
		windowRepository.findTrafficDataByDate(sdf.format(new Date())).forEach(e -> windowTrafficList.add(e));	
		poiRepository.findAll().forEach(e -> poiTrafficList.add(e));
		//prepare response
		Response response = new Response();
		response.setTotalTraffic(totalTrafficList);
		response.setWindowTraffic(windowTrafficList);
		response.setPoiTraffic(poiTrafficList);
		logger.info("Sending to UI "+response);
		//send to ui
		this.template.convertAndSend("/topic/trafficData", response);
	}
	
}
