
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JoltTest {
	static Date date = new Date();
	static long time = date.getTime();
	static Timestamp ts = new Timestamp(time);

	public static void main(String[] args)
			throws IOException, IllegalAccessException, InstantiationException, SQLException, ParseException {
		//transform();

	}

	public static Map transform()
			throws IOException, IllegalAccessException, InstantiationException, SQLException, ParseException {

		MysqlData.report("Executing transform method");
		File file1 = new File("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\json\\sample\\spec.json");
		File file2 = new File("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\json\\sample\\input.json");

		String spec = FileUtils.readFileToString(file1, "UTF-8");
		String input = FileUtils.readFileToString(file2, "UTF-8");
		// List chainrSpecJSON = JsonUtils.filepathToList("/json/sample/spec.json");
		List chainrSpecJSON = JsonUtils.jsonToList(spec);
		Chainr chainr = Chainr.fromSpec(chainrSpecJSON);
		// Object inputJSON = JsonUtils.filepathToObject("/json/sample/input.json");
		Object inputJSON = JsonUtils.jsonToObject(input);
		Object transformedOutput = chainr.transform(inputJSON);
		// System.out.println(JsonUtils.toJsonString(transformedOutput));
		String joltData = JsonUtils.toJsonString(transformedOutput);
		// System.out.println(joltData);
		joltData = joltData.replaceAll("'", "");
		MysqlData.report("Applying Jolt transformation for products data");
		// joltData = joltData.replaceAll("", "");
		Map<String, String> map = new HashMap<String, String>();

		ObjectMapper mapper = new ObjectMapper();
		map = mapper.readValue(joltData, new TypeReference<Map<String, String>>() {
		});
		MysqlData.report("Execution completed of transform method ");
		// return productCompare.mySql(map);
		return map;
	}

	public static Map attTransform()
			throws IOException, IllegalAccessException, InstantiationException, SQLException, ParseException {

		File file3 = new File("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\json\\sample\\spec2.json");
		File file4 = new File("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\json\\sample\\input.json");

		String spec2 = FileUtils.readFileToString(file3, "UTF-8");
		String input2 = FileUtils.readFileToString(file4, "UTF-8");

		List chainrSpecJSON2 = JsonUtils.jsonToList(spec2);

		Chainr chainr2 = Chainr.fromSpec(chainrSpecJSON2);

		Object inputJSON2 = JsonUtils.jsonToObject(input2);
		MysqlData.report("Applying Jolt transformation for attribute data ");
		Object transformedOutput2 = chainr2.transform(inputJSON2);
		String joltData2 = JsonUtils.toJsonString(transformedOutput2);
		Map<String, String> map2 = new HashMap<String, String>();
		ObjectMapper mapper = new ObjectMapper();
		map2 = mapper.readValue(joltData2, new TypeReference<Map<String, String>>() {
		});
		// System.out.println("AttTransform " + map2);
		FileUtils.writeStringToFile(file4, "");
		return map2;
		// return productCompare.mySql(map);
	}

}
