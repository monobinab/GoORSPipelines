package com.syw.ors.pipeline.common;

import java.util.List;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;

public class OrsRequestFilterPredicate implements SerializableFunction<List<KV<String, String>>, Boolean> {

	private static final long serialVersionUID = 1710455633146433070L;

	@Override
	public Boolean apply(List<KV<String, String>> input) {		
		return input!=null && input.size()>0;
	}

}
