package com.syw.ors.common;

import java.util.List;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;

public class OrsIssuanceFilterPredicate implements SerializableFunction<List<KV<String, String>>, Boolean> {

	private static final long serialVersionUID = 1710455644146433070L;

	@Override
	public Boolean apply(List<KV<String, String>> input) {		
		return input!=null && input.size()>0 
				&& !(input.size()==2 && input.get(0)!=null && input.get(0).getKey()!=null 
					&& (input.get(0).getKey().equals("raw_data") || input.get(0).getKey().equals("error_log"))) ;
	}

}
