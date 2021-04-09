package com.dant.app;

import com.dant.exception.RuntimeExceptionMapper;
import com.dant.filter.GsonProvider;
import com.dant.webservices.*;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by pitton on 2017-02-20.
 */
@ApplicationPath("")
public class App extends Application {
	@Override
	public Set<Object> getSingletons() {
		Set<Object> sets = new HashSet<>(1);
		sets.add(TableWS.getInstance());
		sets.add(IndexWS.getInstance());
		sets.add(LinesWS.getInstance());
		sets.add(QueryWS.getInstance());
		sets.add(NodeWS.getInstance());
		sets.add(FileSystemWS.getInstance());
		return sets;
	}

	@Override
	public Set<Class<?>> getClasses() {
		Set<Class<?>> sets = new HashSet<>(1);
		sets.add(GsonProvider.class);
		sets.add(RuntimeExceptionMapper.class);
		return sets;
	}
}
