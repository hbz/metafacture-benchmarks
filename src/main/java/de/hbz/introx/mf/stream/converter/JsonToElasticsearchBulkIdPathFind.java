/*
 *  Copyright 2014-2016 hbz, Fabian Steeg
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package de.hbz.introx.mf.stream.converter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.FluxCommand;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;

/**
 * Add Elasticsearch bulk indexing metadata to JSON input.<br/>
 * Use after {@link JsonEncoder}, before writing.
 *
 * @author Fabian Steeg (fsteeg)
 * @author Jens Wille
 *
 */
@In(String.class)
@Out(String.class)
@FluxCommand("json-to-elasticsearch-bulk")
public class JsonToElasticsearchBulkIdPathFind extends
		DefaultObjectPipe<String, ObjectReceiver<String>> {

	private ObjectMapper mapper = new ObjectMapper();
	private String[] idPath;
	private String type;
	private String index;

	/**
	 * @param idPath The key path of the JSON value to be used as the ID for the record
	 * @param type The Elasticsearch index type
	 * @param index The Elasticsearch index name
	 */
	public JsonToElasticsearchBulkIdPathFind(String[] idPath, String type, String index) {
		this.idPath = idPath;
		this.type = type;
		this.index = index;
	}

	/**
	 * @param idKey The key of the JSON value to be used as the ID for the record
	 * @param type The Elasticsearch index type
	 * @param index The Elasticsearch index name
	 */
	public JsonToElasticsearchBulkIdPathFind(String idKey, String type, String index) {
		this(new String[]{idKey}, type, index);
	}

	/**
	 * @param idKey The key of the JSON value to be used as the ID for the record
	 * @param type The Elasticsearch index type
	 * @param index The Elasticsearch index name
	 * @param entitySeparator The separator between entity names in idKey
	 */
	public JsonToElasticsearchBulkIdPathFind(String idKey, String type, String index, String entitySeparator) {
		this(idKey.split(Pattern.quote(entitySeparator)), type, index);
	}

	@Override
	public void process(String obj) {
		StringWriter stringWriter = new StringWriter();
		try {
			JsonNode json = mapper.readTree(obj);
			Map<String, Object> detailsMap = new HashMap<String, Object>();
			Map<String, Object> indexMap = new HashMap<String, Object>();
			indexMap.put("index", detailsMap);
			detailsMap.put("_id", findId(json));
			detailsMap.put("_type", type);
			detailsMap.put("_index", index);
			mapper.writeValue(stringWriter, indexMap);
			stringWriter.write("\n");
			mapper.writeValue(stringWriter, json);
		} catch (IOException e) {
			e.printStackTrace();
		}
		getReceiver().process(stringWriter.toString());
	}

	private JsonNode findId(JsonNode value) {
		if (idPath.length < 1) {
			return null;
		}

		for (final String key : idPath) {
			value = value.findPath(key);
		}

		return value;
	}

}
