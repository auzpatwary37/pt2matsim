/*
 * *********************************************************************** *
 * project: org.matsim.*                                                   *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2014 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** *
 */

package org.matsim.pt2matsim.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.TransportMode;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.lanes.Lane;
import org.matsim.lanes.Lanes;
import org.matsim.lanes.LanesReader;
import org.matsim.lanes.LanesToLinkAssignment;
import org.matsim.lanes.LanesWriter;
import org.matsim.pt.transitSchedule.api.TransitLine;
import org.matsim.pt.transitSchedule.api.TransitRoute;
import org.matsim.pt.transitSchedule.api.TransitSchedule;
import org.matsim.pt2matsim.config.PublicTransitMappingConfigGroup;
import org.matsim.pt2matsim.mapping.PTMapper;
import org.matsim.pt2matsim.tools.NetworkTools;
import org.matsim.pt2matsim.tools.ScheduleTools;

/**
 * Allows to run an implementation
 * of public transit mapping via config file path.
 *
 * Currently redirects to the only implementation
 * {@link PTMapper}.
 *
 * @author polettif
 */
public final class PublicTransitMapper {

	protected static Logger log = LogManager.getLogger(PublicTransitMapper.class);

	private PublicTransitMapper() {
	}

	/**
	 * Routes the unmapped MATSim Transit Schedule to the network using the file
	 * paths specified in the config. Writes the resulting schedule and network to xml files.<p/>
	 *
	 * @see CreateDefaultPTMapperConfig
	 *
	 * @param args <br/>[0] PublicTransitMapping config file<br/>
	 */
	public static void main(String[] args) {
		if(args.length == 2) {
			run(args[0],args[1]);
		} else {
			throw new IllegalArgumentException("Both config and Public Transit Mapping config file as argument needed");
		}
	}
	
	public static void run(String mainConfig, String ptMapperConfig) {
		PublicTransitMappingConfigGroup config = PublicTransitMappingConfigGroup.loadConfig(ptMapperConfig);
		Config configAll = ConfigUtils.createConfig();
		ConfigUtils.loadConfig(configAll, mainConfig);
		run(configAll,config);
	}

	/**
	 * Routes the unmapped MATSim Transit Schedule to the network using the file
	 * paths specified in the config. Writes the resulting schedule and network to xml files.<p/>
	 *
	 * @see CreateDefaultPTMapperConfig
	 *
	 * @param configFile the PublicTransitMapping config file
	 */
	public static void run(Config configAll, PublicTransitMappingConfigGroup config) {
		// Load config
		PTMapper.matchInfo(configAll, config);
		// Load input schedule and network
		TransitSchedule schedule = config.getInputScheduleFile() == null ? null : ScheduleTools.readTransitSchedule(config.getInputScheduleFile());
		Network network = config.getInputNetworkFile() == null ? null : NetworkTools.readNetwork(config.getInputNetworkFile());
		Lanes lanes = null;
		if(configAll.network().getLaneDefinitionsFile()!=null) {
			Scenario scn = ScenarioUtils.createScenario(ConfigUtils.createConfig());
			new LanesReader(scn).readFile(configAll.network().getLaneDefinitionsFile());
			lanes = scn.getLanes();
		}
		// Run PTMapper
		PTMapper.mapScheduleToNetwork(schedule, network, lanes, config,configAll);
		// or: new PTMapper(schedule, network).run(config);

		// Write the schedule and network to output files (if defined in config)
		if(config.getOutputNetworkFile() != null && config.getOutputScheduleFile() != null) {
			log.info("Writing schedule and network to file...");
			try {
				ScheduleTools.writeTransitSchedule(schedule, config.getOutputScheduleFile());
				NetworkTools.writeNetwork(network, config.getOutputNetworkFile());
				new LanesWriter(lanes).write(configAll.network().getLaneDefinitionsFile().replace(".xml", "_out.xml"));
				checkConsistensy(network,schedule,lanes);
			} catch (Exception e) {
				log.error("Cannot write to output directory!");
			}
			if(config.getOutputStreetNetworkFile() != null) {
				NetworkTools.writeNetwork(NetworkTools.createFilteredNetworkByLinkMode(network, Collections.singleton(TransportMode.car)), config.getOutputStreetNetworkFile());
			}
		} else {
			log.info("No output paths defined, schedule and network are not written to files.");
		}
	}
	public static int[] checkConsistensy(Network net,TransitSchedule ts,Lanes lanes) {
		int wrongRoutes = 0;
		int missingLink = 0;
		int missingConnections = 0;
		int total = 0;
		for(TransitLine tl:ts.getTransitLines().values()){
			for(TransitRoute tr:tl.getRoutes().values()){
				List<Id<Link>> links = new ArrayList<>();
				
				links.add(tr.getRoute().getStartLinkId());
				links.addAll(tr.getRoute().getLinkIds());
				links.add(tr.getRoute().getEndLinkId());
				boolean problem = false;
				for(int i = 0;i<links.size();i++){
					if(!net.getLinks().containsKey(links.get(i))) {
						log.error("Link with id"+links.get(i)+"is not present in the network.");
						missingLink++;
						problem = true;
					}
					if(i<links.size()-1 && lanes.getLanesToLinkAssignments().get(links.get(i))!=null) {
						total++;
						boolean connected = false;
						for(Lane l:lanes.getLanesToLinkAssignments().get(links.get(i)).getLanes().values()){
							if(l.getToLinkIds().contains(links.get(i+1))) {
								connected = true;
								break;
							}
						}
						if(!connected) {
							LanesToLinkAssignment l2l = lanes.getLanesToLinkAssignments().get(links.get(i));
							log.error("Link with id "+links.get(i)+" is not connected to link "+links.get(i+1)+" using any lanes, Adding artificial lane");
							Lane lane = lanes.getFactory().createLane(Id.create(links.get(i).toString()+"_"+links.get(i+1).toString(), Lane.class));
							lane.addToLinkId(links.get(i+1));
							lane.setCapacityVehiclesPerHour(1800);
							lane.setNumberOfRepresentedLanes(1);
							lane.setStartsAtMeterFromLinkEnd(50);
							l2l.addLane(lane);
							missingConnections++;
							problem = true;
						}
					}
				}
				if(problem)wrongRoutes++;
			}
		}
		return new int[] {wrongRoutes,missingLink,missingConnections};
	}
}
