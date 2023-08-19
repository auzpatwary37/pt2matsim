/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2016 by the members listed in the COPYING,        *
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
 * *********************************************************************** */

package org.matsim.pt2matsim.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.matsim.api.core.v01.Id;
import org.matsim.api.core.v01.network.Link;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.router.util.LeastCostPathCalculator;
import org.matsim.core.utils.collections.Tuple;
import org.matsim.lanes.Lane;
import org.matsim.lanes.Lanes;
import org.matsim.lanes.LanesFactory;
import org.matsim.lanes.LanesToLinkAssignment;
import org.matsim.pt.transitSchedule.api.TransitLine;
import org.matsim.pt.transitSchedule.api.TransitRoute;
import org.matsim.pt.transitSchedule.api.TransitRouteStop;
import org.matsim.pt2matsim.mapping.linkCandidateCreation.LinkCandidate;
import org.matsim.pt2matsim.mapping.linkCandidateCreation.LinkCandidateCreator;
import org.matsim.pt2matsim.mapping.networkRouter.ScheduleRouters;
import org.matsim.pt2matsim.mapping.networkRouter.ScheduleRoutersFactory;
import org.matsim.pt2matsim.mapping.pseudoRouter.ArtificialLink;
import org.matsim.pt2matsim.mapping.pseudoRouter.PseudoGraph;
import org.matsim.pt2matsim.mapping.pseudoRouter.PseudoGraphImpl;
import org.matsim.pt2matsim.mapping.pseudoRouter.PseudoRouteStop;
import org.matsim.pt2matsim.mapping.pseudoRouter.PseudoSchedule;
import org.matsim.pt2matsim.mapping.pseudoRouter.PseudoScheduleImpl;

/**
 * Generates and calculates the pseudoRoutes for all the queued
 * transit lines. If no route on the network can be found (or the
 * scheduleTransportMode should not be mapped to the network), artificial
 * links between link candidates are stored to be created later.
 *
 * @author polettif
 */
public class PseudoRoutingImpl implements PseudoRouting {

	protected static Logger log = LogManager.getLogger(PseudoRoutingImpl.class);
	private final Progress progress;

	private static boolean warnMinTravelCost = true;
	private static Queue<TransitLine> queue = new ConcurrentLinkedQueue<>();

	private final LinkCandidateCreator linkCandidates;
	private final ScheduleRouters scheduleRouters;

	private final Set<ArtificialLink> necessaryArtificialLinks = new HashSet<>();

	private final PseudoSchedule threadPseudoSchedule = new PseudoScheduleImpl();
	private double maxTravelCostFactor;

	public PseudoRoutingImpl(ScheduleRoutersFactory scheduleRoutersFactory, LinkCandidateCreator linkCandidates, double maxTravelCostFactor, Progress progress) {
		this.maxTravelCostFactor = maxTravelCostFactor;
		this.scheduleRouters = scheduleRoutersFactory.createInstance();
		this.linkCandidates = linkCandidates;
		this.progress = progress;
	}

	@Override
	public void addTransitLineToQueue(TransitLine transitLine) {
		queue.add(transitLine);
	}

	@Override
	public void run() {
		
		TransitLine transitLine;
		while ((transitLine = queue.poll()) != null) {
			for(TransitRoute transitRoute : transitLine.getRoutes().values()) {
				/* [1]
				  Initiate pseudoGraph and Dijkstra algorithm for the current transitRoute.

				  In the pseudoGraph, all link candidates are represented as nodes and the
				  network paths between link candidates are reduced to a representation edge
				  only storing the travel cost. With the pseudoGraph, the best linkCandidate
				  sequence can be calculated (using Dijkstra). From this sequence, the actual
				  path on the network can be routed later on.
				 */
				PseudoGraph pseudoGraph = new PseudoGraphImpl();

				/* [2]
				  Calculate the shortest paths between each pair of routeStops/ParentStopFacility
				 */
				List<TransitRouteStop> routeStops = transitRoute.getStops();
				for(int i = 0; i < routeStops.size() - 1; i++) {
					Set<LinkCandidate> linkCandidatesCurrent = linkCandidates.getLinkCandidates(routeStops.get(i), transitLine, transitRoute);
					Set<LinkCandidate> linkCandidatesNext = linkCandidates.getLinkCandidates(routeStops.get(i + 1), transitLine, transitRoute);

					double minTravelCost = scheduleRouters.getMinimalTravelCost(routeStops.get(i), routeStops.get(i + 1), transitLine, transitRoute);
					double maxAllowedTravelCost = minTravelCost * maxTravelCostFactor;

					if(minTravelCost == 0 && warnMinTravelCost) {
						log.warn("There are stop pairs where minTravelCost is 0.0! This might happen if two stops are on the same coordinate or if departure and arrival time of two subsequent stops are identical. Further messages are suppressed.");
						warnMinTravelCost = false;
					}
					
					/* [3]
					  Calculate the shortest path between all link candidates.
					 */
					for(LinkCandidate linkCandidateCurrent : linkCandidatesCurrent) {
						for(LinkCandidate linkCandidateNext : linkCandidatesNext) {

							boolean useExistingNetworkLinks = false;
							double pathCost = 2 * maxAllowedTravelCost;
							List<Link> pathLinks = null;

							/* [3.1]
							  If one or both link candidates are loop links we don't have
							  to search a least cost path on the network.
							 */
							if(!linkCandidateCurrent.isLoopLink() && !linkCandidateNext.isLoopLink()) {
								/*
								  Calculate the least cost path on the network
								 */
								LeastCostPathCalculator.Path leastCostPath = scheduleRouters.calcLeastCostPath(linkCandidateCurrent, linkCandidateNext, transitLine, transitRoute);

								if(leastCostPath != null) {
									pathCost = leastCostPath.travelCost;
									pathLinks = leastCostPath.links;
									// if both link candidates are the same, cost should get higher
									if(linkCandidateCurrent.getLink().getId().equals(linkCandidateNext.getLink().getId())) {
										pathCost *= 4;
									}
								}
								useExistingNetworkLinks = pathCost < maxAllowedTravelCost;
							}

							/* [3.2]
							  If a path on the network could be found and its travel cost are
							  below maxAllowedTravelCost, a normal edge is added to the pseudoGraph
							 */
							if(useExistingNetworkLinks) {
								double currentCandidateTravelCost = scheduleRouters.getLinkCandidateTravelCost(linkCandidateCurrent);
								double nextCandidateTravelCost = scheduleRouters.getLinkCandidateTravelCost(linkCandidateNext);
								double edgeWeight = pathCost + 0.5 * currentCandidateTravelCost + 0.5 * nextCandidateTravelCost;

								pseudoGraph.addEdge(i, routeStops.get(i), linkCandidateCurrent, routeStops.get(i + 1), linkCandidateNext, edgeWeight, pathLinks);
							}
							/* [3.2]
							  Create artificial links between two routeStops if:
							  	 - no path on the network could be found
							    - the travel cost of the path are greater than maxAllowedTravelCost

							  Artificial links are created between all LinkCandidates
							  (usually this means between one dummy link for the stop
							  facility and the other linkCandidates).
							 */
							else {
								double currentCandidateTravelCost = scheduleRouters.getLinkCandidateTravelCost(linkCandidateCurrent);
								double nextCandidateTravelCost = scheduleRouters.getLinkCandidateTravelCost(linkCandidateNext);
								double artificialEdgeWeight = maxAllowedTravelCost - 0.5 * currentCandidateTravelCost - 0.5 * nextCandidateTravelCost;

								pseudoGraph.addEdge(i, routeStops.get(i), linkCandidateCurrent, routeStops.get(i + 1), linkCandidateNext, artificialEdgeWeight, null);
							}
						}
					}
				} // - routeStop loop

				/* [4]
				  Finish the pseudoGraph by adding dummy nodes.
				 */
				pseudoGraph.addDummyEdges(routeStops,
						linkCandidates.getLinkCandidates(routeStops.get(0), transitLine, transitRoute),
						linkCandidates.getLinkCandidates(routeStops.get(routeStops.size() - 1), transitLine, transitRoute));

				/* [5]
				  Find the least cost path i.e. the PseudoRouteStop sequence
				 */
				List<PseudoRouteStop> pseudoPath = pseudoGraph.getLeastCostStopSequence();

				if(pseudoPath == null) {
					throw new RuntimeException("PseudoGraph has no path from SOURCE to DESTINATION for transit route " + transitRoute.getId() + " " +
							"on line " + transitLine.getId() + " from \"" + routeStops.get(0).getStopFacility().getName() + "\" " +
							"to \"" + routeStops.get(routeStops.size() - 1).getStopFacility().getName() + "\"");
				} else {
					necessaryArtificialLinks.addAll(pseudoGraph.getArtificialNetworkLinks());
					threadPseudoSchedule.addPseudoRoute(transitLine, transitRoute, pseudoPath, pseudoGraph.getNetworkLinkIds());
				}
				
				progress.update();
			}
		}
	}


	/**
	 * @return a pseudo schedule generated during run()
	 */
	@Override
	public PseudoSchedule getPseudoSchedule() {
		return threadPseudoSchedule;
	}

	/**
	 * Adds the artificial links to the network.
	 *
	 * Not thread safe.
	 */
	@Override
	public void addArtificialLinks(Network network) {
		for(ArtificialLink a : necessaryArtificialLinks) {
			if(!network.getLinks().containsKey(a.getId())) {
				network.addLink(a);
			}
		}
	}
	
	@Override
	public void addArtificialLinks(Network network, Lanes lanes) {
		for(ArtificialLink a : necessaryArtificialLinks) {
			if(!network.getLinks().containsKey(a.getId())) {
				network.addLink(a);
				boolean requireLane = false;
				for(Link inLink:a.getFromNode().getInLinks().values()) {
					if(lanes.getLanesToLinkAssignments().containsKey(inLink.getId())) {
						requireLane = true;
						break;
					}
				}
				if(requireLane == true) {
					LanesFactory lFac = lanes.getFactory();
					for(Link inLink:a.getFromNode().getInLinks().values()) {
						LanesToLinkAssignment l2l = lanes.getLanesToLinkAssignments().get(inLink.getId());
						if(l2l == null) {
							l2l = lFac.createLanesToLinkAssignment(inLink.getId());
							lanes.addLanesToLinkAssignment(l2l);
						}
						
						Map<Id<Link>, Integer> order = orderToLinks(inLink,null);
						for(Link outLink:inLink.getToNode().getOutLinks().values()) {
							Id<Lane> lId = Id.create(inLink.getFromNode().getId().toString()+"-"+inLink.getToNode().getId().toString()+"-"+outLink.getToNode().getId().toString(), Lane.class);
							if(!l2l.getLanes().containsKey(lId)) {
								Lane l = lFac.createLane(lId);
								l.setAlignment(order.get(outLink.getId()));
								l.setCapacityVehiclesPerHour(1800*outLink.getNumberOfLanes());
								l.setStartsAtMeterFromLinkEnd(100);
								l.setNumberOfRepresentedLanes(outLink.getNumberOfLanes());
								l.addToLinkId(outLink.getId());
								l2l.addLane(l);
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param link the from Link for which lanes are to be ordered.
	 * @param restrictions the lane restrictions. if null then will assume no restrictions.
	 * @return
	 */
	public static Map<Id<Link>,Integer> orderToLinks(Link link, Map<Id<Link>,Integer>restrictions) {
		Map<Id<Link>, Integer> order = new HashMap<>();
		TreeMap<Double,Id<Link>> angle = new TreeMap<>();
		List<Tuple<Id<Link>,Double>> angles = new ArrayList<>();
		//System.out.println();
		for(Link l: link.getToNode().getOutLinks().values()) {
			if(restrictions==null || restrictions.containsKey(l.getId()) && restrictions.get(l.getId())!=0) {
				//angle.put( getAngle(link,l),l.getId());
				angles.add(new Tuple<>(l.getId(),getAngle(link,l)));
			}
		}
		
		Collections.sort(angles,new Comparator<Tuple<Id<Link>,Double>>(){

			@Override
			public int compare(Tuple<Id<Link>, Double> o1, Tuple<Id<Link>, Double> o2) {
				 if (o1.getSecond() == o2.getSecond())
			            return 0;
			        else if (o1.getSecond() > o2.getSecond())
			            return 1;
			        else
			            return -1;
				
			}
			
		});
		
		int nPlus = 0;
		int nMinus = 0;
		if(angles.size()%2==0) {
			nPlus = angles.size()/2-1;
			nMinus = -1*angles.size()/2;
		}else {
			nPlus = (angles.size()-1)/2;
			nMinus = -1*(angles.size()-1)/2;
		}
		int j = 0;
		for(int i = nMinus;i<=nPlus;i++) {
			//Entry<Double, Id<Link>> p = angle.pollLastEntry();
			Tuple<Id<Link>,Double> pp = angles.get(j);
			order.put(pp.getFirst(),i);
			j++;
		}
		
		return order;
	}
	
	public static double getAngle(Link l1, Link l2) {
		
		Tuple<Double,Double> vec1 = new Tuple<>(l1.getToNode().getCoord().getX()-l1.getFromNode().getCoord().getX(),l1.getToNode().getCoord().getY()-l1.getFromNode().getCoord().getY());
		Tuple<Double,Double> vec2 = new Tuple<>(l2.getToNode().getCoord().getX()-l2.getFromNode().getCoord().getX(),l2.getToNode().getCoord().getY()-l2.getFromNode().getCoord().getY());
		
		double dot = vec1.getFirst()*vec2.getFirst()+vec1.getSecond()*vec2.getSecond();
		
		double det = vec1.getFirst()*vec2.getSecond() - vec1.getSecond()*vec2.getFirst();    
		double angle = Math.toDegrees(Math.atan2(det, dot));  
		
		return angle;
	}

}
