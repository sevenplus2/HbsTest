import java.awt.Font;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ThresholdingOutputStream;

import com.opencsv.CSVWriter;

public class MysqlData {
	static Connection conn = null;
	static Statement stmt1 = null;
	static Date date = new Date();
	static long time = date.getTime();
	static Timestamp ts = new Timestamp(time);
	static DateFormat dateFormat2 = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss:SS");
	static CSVWriter writer = new CSVWriter(null);
	// static File file = new
	// File("C:\\Users\\admin\\Documents\\NewHCon\\HbaseTest\\report.txt");

	public static void main(String[] args)
			throws IllegalAccessException, InstantiationException, SQLException, ParseException, IOException {
		// csvReport("PID1", "STATUS1", "ATTR1");
		getProductId();
		// getProductData();
		// getAtt();

	}

	public static void report(String data) throws IOException {
		String ts = dateFormat2.format(new Date()).toString();
		String filename = "report.txt";
		FileWriter fw = new FileWriter(filename, true); // the true will append the new data
		fw.write(data + " | " + ts + "\n");// appends the string to the file
		fw.write("++ ");
		fw.close();
	}

	public static void csvReport(String product_id, String status, String attribute) throws IOException {
		String filename = "csvReport.csv";
		FileWriter writer = new FileWriter(filename, true);
		// writer.write(" ");
		// writer.write(',');
		writer.write("\n");
		writer.write(product_id);
		writer.write(',');
		writer.write(status);
		writer.write(',');
		writer.write(attribute);
		writer.write(',');
		writer.close();
	}

	public static void dbConfig(String functionName) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost/wsimcpsn_shopnow";
			conn = DriverManager.getConnection(url, "root", "Alfred_21");
			report("Connection done for method " + functionName);
			// FileUtils.write(file, "Connection done" + ts, Charset.forName("UTF-8"));
		} catch (Exception ex) {
			System.err.println(ex.getMessage());
		}

	}

	public static void connClose() throws SQLException {
		conn.close();
	}

	public static void getProductId()
			throws IllegalAccessException, InstantiationException, SQLException, ParseException, IOException {
		List pIdList = new ArrayList();
		dbConfig(new Object() {
		}.getClass().getEnclosingMethod().getName());
		report("Executing getProductId method");
		stmt1 = (Statement) conn.createStatement();
		String qry = "SELECT product_id FROM products";// ORDER BY ID DESC LIMIT 2;";
		ResultSet rs = stmt1.executeQuery(qry);
		String str = null;
		int i = 0;
		while (rs.next()) {
			str = rs.getString("product_id");
			pIdList.add(str);
			i++;
			// System.out.println(i);
		}
		// FileUtils.writeStringToFile(file, data + ts, Charset.forName("UTF-8"));
		//// str = "KSHEP8RNM4HZSPGF";
		// for (int j = 1; j < 5; j++) {
		// str = pIdList.get(j).toString();
		//// str = "1_" + str;
		//// report("Calling Hbase class");
		//// HbaseTest.run(str);
		// }
		report("Returning productIdList from getProductId method");
		productCompare.getDbData(pIdList);

	}

	public static Map getProductData(String pId)
			throws IllegalAccessException, InstantiationException, SQLException, ParseException, IOException {
		List pIdList = new ArrayList();
		dbConfig(new Object() {
		}.getClass().getEnclosingMethod().getName());
		report("Executing getProductData method");
		stmt1 = (Statement) conn.createStatement();
		String qry = "SELECT * FROM products where product_id='" + pId + "'";// ORDER BY ID DESC LIMIT
																				// 10;";
		ResultSet rs = stmt1.executeQuery(qry);
		report("Executing query to get product data from MySql");
		ResultSetMetaData metaData = rs.getMetaData();
		int count = metaData.getColumnCount(); // number of column
		Map<String, String> hmap = new HashMap<String, String>();

		String columnName[] = new String[count];
		List<String> list = new ArrayList();
		for (int i = 1; i <= count; i++) {
			columnName[i - 1] = metaData.getColumnLabel(i);
			String clName = columnName[i - 1]; // clName =
			// System.out.println(clName);
			list.add(clName);

		}
		while (rs.next()) {
			for (int i = 0; i < list.size() - 1; i++) {
				String value = list.get(i);
				String data = rs.getString(value);
				String col = metaData.getColumnLabel(i + 1);
				// System.out.println(col + " : " + data);
				hmap.put(col, data);
			}

		}
		SortedMap<String, String> colVal = new TreeMap<String, String>(hmap);
		// System.out.println(colVal);
		report("Returning data from getProductData for product id " + pId);
		return colVal;
	}

	public static String[] getCat(String parent, String subCat, String child, String catId)
			throws SQLException, IOException {
		dbConfig(new Object() {
		}.getClass().getEnclosingMethod().getName());
		String[] catArr = new String[10];
		report("Executing getCat method ");
		stmt1 = (Statement) conn.createStatement();
		String qry = "SELECT category,sub_category FROM cat_mapping WHERE child='" + child + "' AND category = '"
				+ parent + "'AND  sub_category IN (SELECT NAME FROM categories WHERE id='" + catId + "') AND\r\n"
				+ "category IN (SELECT NAME FROM categories WHERE id IN (SELECT parent_id FROM categories WHERE id='"
				+ catId + "'))";
		ResultSet rs = stmt1.executeQuery(qry);
		// if (rs.first()) {
		if (rs.next()) {
			String cat = rs.getString("category");
			String sub_cat = rs.getString("sub_category");
			// System.out.println("At getCat method" + cat + " " + sub_cat);
			catArr[1] = cat;
			catArr[2] = sub_cat;
			// System.out.println("while is executed");
			// }

		} else {
			report("Category data is not inserted in mysql");
			report("cat and sub_cat set to null");
			catArr[1] = "nulll";
			catArr[2] = "nulll";
			System.out.println("cat and sub_cat is set to null");

		}

		/*
		 * while (rs.next()) { System.out.println("in while loop"); String cat =
		 * rs.getString("category"); String sub_cat = rs.getString("sub_category");
		 * System.out.println("At getCat method" + cat + " " + sub_cat); catArr[1] =
		 * cat; catArr[2] = sub_cat;
		 * 
		 * }
		 */

		report("Returning data from getCat");
		return catArr;
	}

	public static Map getAtt(String pId) throws SQLException, IOException {
		dbConfig(new Object() {
		}.getClass().getEnclosingMethod().getName());
		report("Executing getAtt method for product id " + pId);
		String qry = "SELECT attributes_value.att_value,attributes.att_group_name FROM attributes,attributes_value WHERE attributes_value.id IN (SELECT att_group_val_id FROM product_attributes WHERE product_id='"
				+ pId + "') AND attributes.id=attributes_value.att_group_id;";

		Map<String, String> mapAtt = new HashMap<String, String>();
		String att_val;
		String att_name;
		stmt1 = (Statement) conn.createStatement();
		ResultSet rs;
		rs = stmt1.executeQuery(qry);
		// while (rs.next()) {
		if (rs.next()) {
			while (rs.next()) {
				att_val = rs.getString("att_value");
				att_name = rs.getString("att_group_name");
				mapAtt.put(att_name, att_val);
			}

		} else {
			report("Attribute data is not inserted in mysql");
			report("att and att_val set to null");
			att_val = "null";
			att_name = "null";
			mapAtt.put(att_name, att_val);
		}
		// System.out.println(mapAtt);
		report("Returning data from getAtt for product id " + pId);
		return mapAtt;
	}

}
