package com.example.iotdashboard.dao;

import com.example.iotdashboard.dao.entity.POITrafficData;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface POITrafficDataRepository extends CassandraRepository<POITrafficData>{
	 
}
