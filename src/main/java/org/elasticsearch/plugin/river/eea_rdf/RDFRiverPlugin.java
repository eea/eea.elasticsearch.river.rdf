package org.elasticsearch.plugin.river.eea_rdf.eea_rdf_river_plugin;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.eea_rdf.RDFRiverModule;

public class RDFRiverPlugin extends AbstractPlugin {

	@Inject
  public RDFRiverPlugin(){
	}

	@Override
	public String name() {
		return "eea-rdf-river";
	}

	@Override
  public String description() {
		return "Turtle RDF River Plugin";
	}

	public void onModule(RiversModule module) {
		module.registerRiver("eeaRDF", RDFRiverModule.class);
	}
}

