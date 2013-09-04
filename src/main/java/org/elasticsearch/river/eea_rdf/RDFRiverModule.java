package org.elasticsearch.river.eea_rdf;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class RDFRiverModule extends AbstractModule {
		@Override
		public void configure(){
			bind(River.class).to(RDFRiver.class).asEagerSingleton();
		}
}
