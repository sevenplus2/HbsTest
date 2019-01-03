
import java.awt.Font;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SystemUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class productCompare {
	static Date date = new Date();
	static long time = date.getTime();
	static Timestamp ts = new Timestamp(time);
	static Map<String, String> mySqlMap = new HashMap<String, String>();
	static Map<String, String> joltMap = new HashMap<String, String>();
	static String pId;

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException,
			IllegalAccessException, InstantiationException, SQLException, ParseException {
		// mySql();
		// attCmp(null);

		// getDbData();

	}

	public static String[] getCsv(String filePath) throws IOException {
		File csvFile = new File(filePath);
		String csvData = FileUtils.readFileToString(csvFile, "UTF-8");

		csvData = csvData.replaceAll("\\n", "");
		csvData = csvData.replaceAll("\\r", ",");
		csvData = csvData.substring(0, csvData.lastIndexOf(","));
		String[] csvArray = csvData.split(",");
		return csvArray;
	}

	public static void getDbData(List pIdList)
			// public static void getDbData()
			throws IllegalAccessException, InstantiationException, IOException, SQLException, ParseException {
		pId = null;
		System.out.println("pIdList size " + pIdList.size());
		for (int i = 0; i < pIdList.size(); i++) {
			// for (int i = 0; i < 10; i++) {
			pId = pIdList.get(i).toString();
			//pId = "ABQE3R7H7NRZTER4";
			MysqlData.report(i + 1 + "] " + "\n");
			System.out.println(i + "]");
			HbaseTest.run(pId);
			mySqlMap = MysqlData.getProductData(pId);
			joltMap = JoltTest.transform();
			System.out.println("executing cmp");
			cmp(mySqlMap, joltMap);
			System.out.println("executing catPath");
			catPath(mySqlMap, joltMap);
			// pId = mySqlMap.get("product_id");
			System.out.println("attCmp");
			attCmp(pId);
			MysqlData.report("\n" + "===========================" + ts);
			MysqlData.connClose();

		}
	}

	public static void catPath(Map<String, String> mySqlMap, Map<String, String> joltMap)
			throws SQLException, IOException {
		// System.out.println(mySqlMap.get("category_id"));
		String catId = mySqlMap.get("category_id");
		String catPath = joltMap.get("categoryPath");
		String pId = "1_" + mySqlMap.get("product_id");
		String subCat = null;
		String cPath = catPath;
		System.out.println("cPath " + cPath);
		String parent = null;
		String child = null;
		String cMap = null;// Subcategory from cat_path
		int i = 0;
		int index = 0;
		int find = 0;
		Integer[] a = new Integer[10];
		for (i = 0; i < cPath.length(); i++) {
			if (cPath.charAt(i) == '>') {
				find++;
				a[index] = i;
				// System.out.println(a[index]);
				index++;
			}
		}
		try {

			if (cPath.contains("Men")) {
				parent = "Men";
			}
			if (cPath.contains("Women")) {
				parent = "Women";
			}
			if (cPath.contains("Boys")) {
				parent = "Boys";
			}
			if (cPath.contains("Girls")) {
				parent = "Girls";
			}
			child = cPath.substring(cPath.lastIndexOf(">") + 1, cPath.length());

			if (parent == null) {
				parent = cPath;
			}
			if (child == null) {
				child = cPath;
			}
			if (find >= 3) {
				if (cPath.contains("Infant") || cPath.contains("Kids")) {
					cMap = cPath.substring((a[index - 2]) + 1, a[index - 1]);
					// System.out.println(cMap);
				} else {
					cMap = cPath.substring(a[1] + 1, a[2]);
					// System.out.println(cMap);
				}
			} else {
				cMap = child;
				subCat = cMap;
			}
			subCat = cMap;
			System.out.println("cat" + "path " + "Parent " + parent + " Sub_cat " + cMap + " Child " + child);

			// System.out.println("Before calling getCat");
			String[] getArr = MysqlData.getCat(parent, subCat, child, catId);
			String mysqlCat = getArr[1];
			String mysqlSubCat = getArr[2];

			// System.out.println("\nCat " + mysqlCat + " | joltCat " + parent);
			// System.out.println("SubCat " + mysqlSubCat + " | joltSubCat" + cMap);
			// System.out.println("child " + child);

			if (mysqlCat.equalsIgnoreCase(parent)) {
				MysqlData.report("parent_cat from mySql \"" + mysqlCat + "\" and from hbase \"" + parent
						+ "\" is matched for product id " + pId);
			} else {
				// System.out.println("\n Cat " + mysqlCat + " |joltCat " + parent);
				MysqlData.csvReport(pId, "Not Matched", "parent_cat");
				MysqlData.report("parent_cat from mysql \"" + mysqlCat + "\" and from hbase \"" + parent
						+ "\" is not matched for product id " + pId);

			}

			if (mysqlSubCat.equalsIgnoreCase(cMap)) {
				MysqlData.report("sub_cat from mysql \"" + mysqlSubCat + "\" and from hbase \"" + cMap
						+ "\" is matched for product id \"" + pId);
			} else {
				// System.out.println("SubCat " + mysqlSubCat + " |joltSubCat" + cMap);
				MysqlData.csvReport(pId, "Not Matched", "sub_cat");
				MysqlData.report("sub_cat from mysql \"" + mysqlSubCat + "\" and from hbase \"" + cMap
						+ "\" is not matched for product id " + pId);
			}
		} catch (Exception e) {
			System.out.println("Exception occured " + e);
		}

		// System.out.println("mySqlCatSubCat" + mysqlCat + " " + mysqlSubCat);
	}

	public static void attCmp(String pId)
			throws SQLException, IllegalAccessException, InstantiationException, IOException, ParseException {
		// pId = "KSHEP8RNM4HZSPGF";
		MysqlData.report("Comparing productAttribute for product id " + pId + " | " + ts);
		String[] csvArray = getCsv("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\attributes.csv");

		mySqlMap = MysqlData.getAtt(pId);
		joltMap = JoltTest.attTransform();
		System.out.println("Attributes in mysqlMap " + mySqlMap);
		System.out.println("Attributes in joltMap " + joltMap);
		try {
			for (int i = 0; i <= csvArray.length - 1; i++) {

				String key = csvArray[i];
				String mySqlString_val = mySqlMap.get(key);
				String joltString_val = joltMap.get(key);

				if (mySqlString_val == null || mySqlString_val.isEmpty()) {
					mySqlString_val = "0";
					MysqlData.report(key + "_att of mysqlMap is set to 0 for product id " + pId);

				}
				if (joltString_val == null || joltString_val.isEmpty()) {
					joltString_val = "0";
					MysqlData.report(key + "_att of mysqlMap is set to 0 for product id " + pId);

				}

				if (mySqlString_val.equalsIgnoreCase(joltString_val)) {
					MysqlData.report(key + "_att from mysql \"" + mySqlString_val + "\" and from jolt \""
							+ joltString_val + "\" is matched for product id " + pId);
				} else {
					MysqlData.csvReport(pId, "Not Matched", key + "_att");
					MysqlData.report(key + "_att from mysql \"" + mySqlString_val + "\" and from jolt \""
							+ joltString_val + "\" is not matched for product id " + pId);
				}
			}

		} catch (Exception e) {

			System.out.println("Exception occured " + e);

		}
	}

	public static void cmp(Map<String, String> mySqlMap, Map<String, String> joltMap) throws IOException {
		MysqlData.report("Comparing products data " + ts);
		String[] csvArray = getCsv("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\productData.csv");
		try {
			for (int i = 0; i <= csvArray.length - 1; i++) {

				String key = csvArray[i];
				String mySqlString_val = mySqlMap.get(key);
				String joltString_val = joltMap.get(key);

				if (key.equalsIgnoreCase("regular_price")) {
					float f = Float.parseFloat(mySqlString_val);
					mySqlString_val = Float.toString(f);

				} else if (key.equalsIgnoreCase("sale_price")) {
					float f = Float.parseFloat(mySqlString_val);
					mySqlString_val = Float.toString(f);
				} else if (key.equalsIgnoreCase("product_id")) {
					mySqlString_val = "1_" + mySqlString_val;
				}

				if (mySqlString_val.equalsIgnoreCase(joltString_val)) {
					MysqlData.report(key + " is matched for product id " + pId);
					// System.out.println(key + " is matched for product id " + pId);

				} else {
					MysqlData.csvReport(pId, "Not Matched", "parent_cat");
					MysqlData.report(key + " is not matched for product id " + pId);

					/*
					 * MysqlData.report(key + " from mysql \"" + mySqlString_val +
					 * "\" and from jolt \"" + joltString_val + "\" is not matched for product id "
					 * + pId);
					 */
				}
			}
		} catch (Exception e) {
			System.out.println("Exception occured " + e);
		}

	}

}
