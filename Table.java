
/****************************************************************************************
 * @file  Table.java
 *
 * @author   John Miller
 */

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

import static java.lang.Boolean.*;
import static java.lang.System.out;

/****************************************************************************************
 * This class implements relational database tables (including attribute names,
 * domains and a list of tuples. Five basic relational algebra operators are
 * provided: project, select, union, minus and join. The insert data
 * manipulation operator is also provided. Missing are update and delete data
 * manipulation operators.
 */
public class Table implements Serializable {
	/**
	 * Relative path for storage directory
	 */
	private static final String DIR = "store" + File.separator;

	/**
	 * Filename extension for database files
	 */
	private static final String EXT = ".dbf";

	/**
	 * Counter for naming temporary tables.
	 */
	private static int count = 0;

	/**
	 * Table name.
	 */
	private final String name;

	/**
	 * Array of attribute names.
	 */
	private final String[] attribute;

	/**
	 * Array of attribute domains: a domain may be integer types: Long, Integer,
	 * Short, Byte real types: Double, Float string types: Character, String
	 */
	private final Class[] domain;

	/**
	 * Collection of tuples (data storage).
	 */
	private final List<Comparable[]> tuples;

	/**
	 * Primary key.
	 */
	private final String[] key;

	/**
	 * Index into tuples (maps key to tuple number).
	 */
	private final Map<KeyType, Comparable[]> index;

	/**
	 * The supported map types.
	 */
	private enum MapType {
		NO_MAP, TREE_MAP, HASH_MAP, LINHASH_MAP
	}

	/**
	 * The map type to be used for indices. Change as needed.
	 */
	private static final MapType mType = MapType.LINHASH_MAP;

	/************************************************************************************
	 * Make a map (index) given the MapType.
	 */
	private static Map<KeyType, Comparable[]> makeMap() {
		return switch (mType) {
			case TREE_MAP -> new TreeMap<>();
			case HASH_MAP -> new HashMap<>();
			case LINHASH_MAP -> new LinHashMap<>(KeyType.class, Comparable[].class);
			// case BPTREE_MAP -> new BpTreeMap <> (KeyType.class, Comparable [].class);
			default -> null;
		}; // switch
	} // makeMap

	// -----------------------------------------------------------------------------------
	// Constructors
	// -----------------------------------------------------------------------------------

	/************************************************************************************
	 * Construct an empty table from the meta-data specifications.
	 *
	 * @param _name      the name of the relation
	 * @param _attribute the string containing attributes names
	 * @param _domain    the string containing attribute domains (data types)
	 * @param _key       the primary key
	 */
	public Table(String _name, String[] _attribute, Class[] _domain, String[] _key) {
		name = _name;
		attribute = _attribute;
		domain = _domain;
		key = _key;
		tuples = new ArrayList<>();
		index = makeMap();

	} // primary constructor

	/************************************************************************************
	 * Construct a table from the meta-data specifications and data in _tuples list.
	 *
	 * @param _name      the name of the relation
	 * @param _attribute the string containing attributes names
	 * @param _domain    the string containing attribute domains (data types)
	 * @param _key       the primary key
	 * @param _tuples    the list of tuples containing the data
	 */
	public Table(String _name, String[] _attribute, Class[] _domain, String[] _key, List<Comparable[]> _tuples) {
		name = _name;
		attribute = _attribute;
		domain = _domain;
		key = _key;
		tuples = _tuples;
		index = makeMap();
	} // constructor

	/************************************************************************************
	 * Construct an empty table from the raw string specifications.
	 *
	 * @param _name      the name of the relation
	 * @param attributes the string containing attributes names
	 * @param domains    the string containing attribute domains (data types)
	 * @param _key       the primary key
	 */
	public Table(String _name, String attributes, String domains, String _key) {
		this(_name, attributes.split(" "), findClass(domains.split(" ")), _key.split(" "));

		out.println("DDL> create table " + name + " (" + attributes + ")");
	} // constructor

	// ----------------------------------------------------------------------------------
	// Public Methods
	// ----------------------------------------------------------------------------------

	/************************************************************************************
	 * Project the tuples onto a lower dimension by keeping only the given
	 * attributes. Check whether the original key is included in the projection.
	 *
	 * #usage movie.project ("title year studioNo")
	 *
	 * @param attributes the attributes to project onto
	 * @return a table of projected tuples
	 * @author Sanskruti Reddy Donthi, Astha Jain
	 */
	public Table project(String attributes) {
		out.println("RA> " + name + ".project (" + attributes + ")");
		var attrs = attributes.split(" ");
		var colDomain = extractDom(match(attrs), domain);
		var newKey = (Arrays.asList(attrs).containsAll(Arrays.asList(key))) ? key : attrs;

		List<Comparable[]> rows = new ArrayList<>();

		// T O B E I M P L E M E N T E D
		for (int i = 0; i < tuples.size(); i++) {
			// Extracting the tuples with the particular columns required
			Comparable[] colArray = extract(tuples.get(i), attrs);
			for (int k = 0; k < rows.size(); k++) {
				// removing duplicates from the extracted values
				if (new KeyType(rows.get(k)).equals(new KeyType(colArray))) {
					rows.remove(rows.get(k));
				}
			}
			// adding the extracted tuples to rows
			rows.add(colArray);
		}

		return new Table(name + count++, attrs, colDomain, newKey, rows);
	} // project

	/************************************************************************************
	 * Select the tuples satisfying the given predicate (Boolean function).
	 *
	 * #usage movie.select (t -> t[movie.col("year")].equals (1977))
	 *
	 * @param predicate the check condition for tuples
	 * @return a table with tuples satisfying the predicate
	 */
	public Table select(Predicate<Comparable[]> predicate) {
		out.println("RA> " + name + ".select (" + predicate + ")");

		return new Table(name + count++, attribute, domain, key,
				tuples.stream().filter(t -> predicate.test(t)).collect(Collectors.toList()));
	} // select

	/************************************************************************************
	 * Select the tuples satisfying the given key predicate (key = value). Use an
	 * index (Map) to retrieve the tuple with the given key value.
	 *
	 * @param keyVal the given key value
	 * @return a table with the tuple satisfying the key predicate
	 * @author Astha Jain
	 */
	public Table select(KeyType keyVal) {
		out.println("RA> " + name + ".select (" + keyVal + ")");

		List<Comparable[]> rows = new ArrayList<>();

		// T O B E I M P L E M E N T E D
		Comparable[] tempRows = null;

		if (mType == MapType.NO_MAP) {
			out.println("please select a map");
		} else {
			tempRows = index.get(keyVal);
		}
		if (tempRows != null) {
			// Adding all the tuples that has keyVal as primary key in the table
			rows.add(tempRows);
		} // if

		return new Table(name + count++, attribute, domain, key, rows);
	} // select

	/************************************************************************************
	 * Select the tuples satisfying the given key predicate (key = value). Retrieve
	 * using a linear scan.
	 *
	 * @param keyVal the given key value
	 * @return a table with the tuple satisfying the key predicate
	 */
	public Table nonIndexSelect(KeyType keyVal) {
		ArrayList<Integer> keyIndexes = new ArrayList<Integer>();
		HashSet keyNames = new HashSet(Arrays.asList(key));
		for (int i = 0; i < attribute.length; i++) {
			if (keyNames.contains(attribute[i])) {
				keyIndexes.add(i);
			}
		}
		List<Comparable[]> rows = new ArrayList<>();
		for (int i = 0; i < tuples.size(); i++) {
			Comparable[] currentTuple = tuples.get(i);
			List<Comparable> keyValues = new ArrayList<Comparable>();
			for (int j = 0; j < keyIndexes.size(); j++) {
				keyValues.add(currentTuple[keyIndexes.get(j)]);
			}
			KeyType keyToCompare = new KeyType(keyValues.toArray(new Comparable[0]));
			if (keyToCompare.equals(keyVal)) {
				rows.add(tuples.get(i));
			}
		}

		return new Table(name + count++, attribute, domain, key, rows);
	} // select

	/************************************************************************************
	 * Union this table and table2. Check that the two tables are compatible.
	 *
	 * #usage movie.union (show)
	 *
	 * @param table2 the rhs table in the union operation
	 * @return a table representing the union
	 * @author Preeti Chatterjee
	 */
	public Table union(Table table2) {
		out.println("RA> " + name + ".union (" + table2.name + ")");

		List<Comparable[]> rows = new ArrayList<>();

		// T O B E I M P L E M E N T E D
		// checking compatibility between tables
		if (compatible(table2)) {
			// if compatible, we are adding all available tuples from first table
			rows.addAll(tuples);
			// comparing with table 2's tuples
			for (Comparable[] t : table2.tuples) {
				// if we don't detect duplicates
				if (!rows.contains(t)) {
					// we add that row from Table 2 to our new list and complete the union
					rows.add(t);
				}
			}
		}

		return new Table(name + count++, attribute, domain, key, rows);
	} // union

	/************************************************************************************
	 * Take the difference of this table and table2. Check that the two tables are
	 * compatible.
	 *
	 * #usage movie.minus (show)
	 *
	 * @param table2 The rhs table in the minus operation
	 * @return a table representing the difference
	 * @author Preeti Chatterjee
	 */
	public Table minus(Table table2) {
		out.println("RA> " + name + ".minus (" + table2.name + ")");
		List<Comparable[]> rows = new ArrayList<>();

		// T O B E I M P L E M E N T E D
		// checking compatibility between tables
		if (compatible(table2)) {
			// comparing with table 2's tuples
			for (Comparable[] t : tuples) {
				// if the tuples in table one are different from table 2
				if (!table2.tuples.contains(t)) {
					// we'll add those tuples otherwise we'll ignore them and hence creating a new
					// table which has tuples unique to Table 1 and which has nothing in common with
					// Table 2
					rows.add(t);
				}
			}
		}

		return new Table(name + count++, attribute, domain, key, rows);
	} // minus

	/************************************************************************************
	 * Join this table and table2 by performing an "equi-join". Same as above, but
	 * implemented using an Index Join algorithm.
	 *
	 * @param attribute1 the attributes of this table to be compared (Foreign Key)
	 * @param attribute2 the attributes of table2 to be compared (Primary Key)
	 * @param table2     the rhs table in the join operation
	 * @return a table with tuples satisfying the equality predicate
	 */
	public Table i_join(String attributes1, String attributes2, Table table2) {
		out.println("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", " + table2.name + ")");

		var t_attrs = attributes1.split(" ");
		var u_attrs = attributes2.split(" ");
		var rows = new ArrayList<Comparable[]>();

		// T O B E I M P L E M E N T E D
		// To check if primary key and foreign key relationship is satisfied or not
		int count1 = 0;
		for (int i = 0; i < t_attrs.length; i++) {
			if (Arrays.asList(attribute).contains(t_attrs[i])) {
				count1++;
			}
		}
		int count2 = 0;
		for (int i = 0; i < u_attrs.length; i++) {
			if (Arrays.asList(table2.key).contains(u_attrs[i])) {
				count2++;
			}
		}

		// Perform join on valid key types
		if (count1 == t_attrs.length && count2 == u_attrs.length) {
			for (int i = 0; i < tuples.size(); i++) {

				// Getting keyType for foreign key of table1 for comparing with primary key of
				// table2
				KeyType keyTypeTable1 = new KeyType(extract(tuples.get(i), t_attrs));

				// Fetching table2 tuples that matches primary key with foreign key of table1
				Comparable[] tuplesTable2 = table2.index.get(keyTypeTable1);

				// Null pointer check in case no key matches the condition
				if (tuplesTable2 == null) {
					continue;
				}

				// Initializing an empty tuple having a size of columns of table1 plus table2
				Comparable[] joinRow = new Comparable[tuples.get(i).length + tuplesTable2.length];

				// Concatenating two arrays - tuple array from table1 and tuple array from
				// table2 to joinRow tuple using ArrayUtil class
				joinRow = ArrayUtil.concat(tuples.get(i), tuplesTable2);

				// Adding joinRow to rows
				rows.add(joinRow);
			}
		} else {
			System.out.println("ERROR: Please enter valid column names");
		}

		// Appending ambiguous column name with 2
		for (int i = 0; i < attribute.length; i++) {
			for (int j = 0; j < table2.attribute.length; j++) {
				if (attribute[i].equals(table2.attribute[j])) {
					table2.attribute[j] += "2";
				}
			}
		}

		return new Table(name + count++, ArrayUtil.concat(attribute, table2.attribute),
				ArrayUtil.concat(domain, table2.domain), key, rows);
	} // i_join

	// /************************************************************************************
	// * Join this table and table2 by performing an "equi-join". Same as above, but
	// implemented
	// * using a Hash Join algorithm.
	// *
	// * @param attribute1 the attributes of this table to be compared (Foreign Key)
	// * @param attribute2 the attributes of table2 to be compared (Primary Key)
	// * @param table2 the rhs table in the join operation
	// * @return a table with tuples satisfying the equality predicate
	// */
	// public Table h_join (String attributes1, String attributes2, Table table2)
	// {
	//
	// // D O N O T I M P L E M E N T
	//
	// return null;
	// } // h_join

	/************************************************************************************
	 * Join this table and table2 by performing an "equi-join". Tuples from both
	 * tables are compared requiring attributes1 to equal attributes2. Disambiguate
	 * attribute names by append "2" to the end of any duplicate attribute name.
	 *
	 * @author Lin Zhao
	 *
	 *         #usage movie.join ("studioNo", "name", studio)
	 *
	 * @param attribute1 the attributes of this table to be compared (Foreign Key)
	 * @param attribute2 the attributes of table2 to be compared (Primary Key)
	 * @param table2     the rhs table in the join operation
	 * @return a table with tuples satisfying the equality predicate
	 */
	public Table equi_join(String attributes1, String attributes2, Table table2) {
		out.println("RA> " + name + ".join (" + attributes1 + ", " + attributes2 + ", " + table2.name + ")");

		String[] t_attrs = attributes1.split(" ");
		String[] u_attrs = attributes2.split(" ");

		List<Comparable[]> rows = new ArrayList<>();

		if (t_attrs.length != u_attrs.length) {
			System.out.println("Cannot Perform Join Operator");
			return null;
		}

		for (Comparable[] tuple1 : tuples) {
			for (Comparable[] tuple2 : table2.tuples) {

				Comparable[] Attri1 = this.extract(tuple1, t_attrs);
				Comparable[] Attri2 = table2.extract(tuple2, u_attrs);

				boolean flag = true;

				// Judge if attributes1 in table1 is equal to attributes2 in table 2
				for (int i = 0; i < Attri1.length; i++) {

					if (!Attri1[i].equals(Attri2[i])) {
						flag = false;
						break;
					}
				}

				// Concatenate tuples from table1&2 to form a new tuple
				if (flag) {
					Comparable[] join_tuple = ArrayUtil.concat(tuple1, tuple2);
					rows.add(join_tuple);
				}
			}
		}

		// Disambiguate attribute names by append "2" to the end of any duplicate
		// attribute name.
		// Here we just need to rename the attribute names in table2 then concatenate
		// them to those in table1
		String[] attribute2_new = table2.attribute;

		for (int j = 0; j < t_attrs.length; j++) {
			for (int k = 0; k < attribute2_new.length; ++k) {

				if (attribute2_new[k].equals(t_attrs[j])) {

					String tmp_attri = t_attrs[j] + "2";
					attribute2_new[j] = tmp_attri;
				}
			}
		}

		return new Table(name + count++, ArrayUtil.concat(attribute, attribute2_new),
				ArrayUtil.concat(domain, table2.domain), key, rows);
	} // join

	/************************************************************************************
	 * Join this table and table2 by performing an "natural join". Tuples from both
	 * tables are compared requiring common attributes to be equal. The duplicate
	 * column is also eliminated.
	 *
	 * #usage movieStar.join (starsIn)
	 *
	 * @param table2 the rhs table in the join operation
	 * @return a table with tuples satisfying the equality predicate
	 */
	public Table join(Table table2) {
		out.println("RA> " + name + ".join (" + table2.name + ")");

		var rows = new ArrayList<Comparable[]>();

		// T O B E I M P L E M E N T E D
		String commonAttr = "";

		// Finding common attributes for the natural join
		for (int i = 0; i < attribute.length; i++) {
			for (int j = 0; j < table2.attribute.length; j++) {
				if (attribute[i].equals(table2.attribute[j]) && domain[j].equals(table2.domain[i])) {
					commonAttr += attribute[i] + " ";
				}
			}
		}

		// Trimming the extra space
		commonAttr.trim();

		// Defining new attribute arrays
		String[] attrs = commonAttr.split(" ");
		int uncommonAttrsLength = table2.attribute.length - attrs.length;
		String[] attrs2 = new String[uncommonAttrsLength];

		// Perform join only if common attributes exist
		if (attrs.length > 0 && attrs[0] != "") {
			for (int i = 0; i < tuples.size(); i++) {

				// Defining new tuples to join table 1 and 2
				Comparable[] tuplesTable1 = new Comparable[attribute.length];
				Comparable[] tuplesTable2 = new Comparable[table2.attribute.length - attrs.length];

				// Flag to set only when tuple values match with common columns
				boolean flag = false;

				// Fetching tuples on common attributes from table 1
				KeyType keyTypeTable1 = new KeyType(extract(tuples.get(i), attrs));

				for (int j = 0; j < table2.tuples.size(); j++) {

					// Fetching tuples on common attributes from table 2
					KeyType keyTypeTable2 = new KeyType(table2.extract(table2.tuples.get(j), attrs));

					// Finding uncommon attributes in table2
					if (uncommonAttrsLength > 0) {
						String uncommonAttrs = "";
						for (int k = 0; k < table2.attribute.length; k++) {
							if (!Arrays.asList(attrs).contains(table2.attribute[k])) {
								uncommonAttrs += table2.attribute[k] + " ";
							}
						}
						uncommonAttrs.trim();
						attrs2 = uncommonAttrs.split(" ");
					}

					// Setting (join) tuple values if the tuple values in both the tables match
					if (keyTypeTable1.equals(keyTypeTable2)) {
						flag = true;
						tuplesTable1 = tuples.get(i);
						if (attrs2.length > 0)
							tuplesTable2 = table2.extract(table2.tuples.get(j), attrs2);
					}
				}

				// Do nothing if tuples don't match
				if (!flag) {
					continue;
				}

				// Initializing an empty tuple having a size of columns of table1 plus table2
				Comparable[] joinRow = new Comparable[tuplesTable1.length + tuplesTable2.length];

				// Concatenating two arrays - tuple array from table1 and tuple array from
				// table2 to joinRow tuple using ArrayUtil class
				joinRow = ArrayUtil.concat(tuplesTable1, tuplesTable2);

				// Adding joinRow to rows
				rows.add(joinRow);

			}
			// Otherwise only print empty table with column names
		} else {
			attrs2 = table2.attribute;
		}

		// FIX - eliminate duplicate columns
		return new Table(name + count++, ArrayUtil.concat(attribute, table2.attribute),
				ArrayUtil.concat(domain, table2.domain), key, rows);
	} // join

	/************************************************************************************
	 * Return the column position for the given attribute name.
	 *
	 * @param attr the given attribute name
	 * @return a column position
	 */
	public int col(String attr) {
		for (var i = 0; i < attribute.length; i++) {
			if (attr.equals(attribute[i]))
				return i;
		} // for

		return -1; // not found
	} // col

	/************************************************************************************
	 * Insert a tuple to the table.
	 *
	 * #usage movie.insert ("'Star_Wars'", 1977, 124, "T", "Fox", 12345)
	 *
	 * @param tup the array of attribute values forming the tuple
	 * @return whether insertion was successful
	 */
	public boolean insert(Comparable[] tup) {
		//out.println("DML> insert into " + name + " values ( " + Arrays.toString(tup) + " )");

		if (typeCheck(tup)) {
			tuples.add(tup);
			var keyVal = new Comparable[key.length];
			var cols = match(key);
			for (var j = 0; j < keyVal.length; j++)
				keyVal[j] = tup[cols[j]];
			if (mType != MapType.NO_MAP)
			{System.out.println(new KeyType(keyVal) == null);
				index.put(new KeyType(keyVal), tup);
			}
			return true;
		} else {
			return false;
		} // if
	} // insert

	/************************************************************************************
	 * Get the name of the table.
	 *
	 * @return the table's name
	 */
	public String getName() {
		return name;
	} // getName

	/************************************************************************************
	 * Print this table.
	 */
	public void print() {
		out.println("\n Table " + name);
		out.print("|-");
		out.print("---------------".repeat(attribute.length));
		out.println("-|");
		out.print("| ");
		for (var a : attribute)
			out.printf("%15s", a);
		out.println(" |");
		out.print("|-");
		out.print("---------------".repeat(attribute.length));
		out.println("-|");
		for (var tup : tuples) {
			out.print("| ");
			for (var attr : tup)
				out.printf("%15s", attr);
			out.println(" |");
		} // for
		out.print("|-");
		out.print("---------------".repeat(attribute.length));
		out.println("-|");
	} // print

	/************************************************************************************
	 * Print this table's index (Map).
	 */
	public void printIndex() {
		out.println("\n Index for " + name);
		out.println("-------------------");
		if (mType != MapType.NO_MAP) {
			for (var e : index.entrySet()) {
				out.println(e.getKey() + " -> " + Arrays.toString(e.getValue()));
			} // for
		} // if
		out.println("-------------------");
	} // printIndex

	/************************************************************************************
	 * Load the table with the given name into memory.
	 *
	 * @param name the name of the table to load
	 */
	public static Table load(String name) {
		Table tab = null;
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(DIR + name + EXT));
			tab = (Table) ois.readObject();
			ois.close();
		} catch (IOException ex) {
			out.println("load: IO Exception");
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			out.println("load: Class Not Found Exception");
			ex.printStackTrace();
		} // try
		return tab;
	} // load

	/************************************************************************************
	 * Save this table in a file.
	 */
	public void save() {
		try {
			var oos = new ObjectOutputStream(new FileOutputStream(DIR + name + EXT));
			oos.writeObject(this);
			oos.close();
		} catch (IOException ex) {
			out.println("save: IO Exception");
			ex.printStackTrace();
		} // try
	} // save

	// ----------------------------------------------------------------------------------
	// Private Methods
	// ----------------------------------------------------------------------------------

	/************************************************************************************
	 * Determine whether the two tables (this and table2) are compatible, i.e., have
	 * the same number of attributes each with the same corresponding domain.
	 *
	 * @param table2 the rhs table
	 * @return whether the two tables are compatible
	 */
	private boolean compatible(Table table2) {
		if (domain.length != table2.domain.length) {
			out.println("compatible ERROR: table have different arity");
			return false;
		} // if
		for (var j = 0; j < domain.length; j++) {
			if (domain[j] != table2.domain[j]) {
				out.println("compatible ERROR: tables disagree on domain " + j);
				return false;
			} // if
		} // for
		return true;
	} // compatible

	/************************************************************************************
	 * Match the column and attribute names to determine the domains.
	 *
	 * @param column the array of column names
	 * @return an array of column index positions
	 */
	private int[] match(String[] column) {
		int[] colPos = new int[column.length];

		for (var j = 0; j < column.length; j++) {
			var matched = false;
			for (var k = 0; k < attribute.length; k++) {
				if (column[j].equals(attribute[k])) {
					matched = true;
					colPos[j] = k;
				} // for
			} // for
			if (!matched) {
				out.println("match: domain not found for " + column[j]);
			} // if
		} // for

		return colPos;
	} // match

	/************************************************************************************
	 * Extract the attributes specified by the column array from tuple t.
	 *
	 * @param t      the tuple to extract from
	 * @param column the array of column names
	 * @return a smaller tuple extracted from tuple t
	 */
	private Comparable[] extract(Comparable[] t, String[] column) {
		var tup = new Comparable[column.length];
		var colPos = match(column);
		for (var j = 0; j < column.length; j++)
			tup[j] = t[colPos[j]];
		return tup;
	} // extract

	/************************************************************************************
	 * Check the size of the tuple (number of elements in list) as well as the type
	 * of each value to ensure it is from the right domain.
	 *
	 * @param t the tuple as a list of attribute values
	 * @return whether the tuple has the right size and values that comply with the
	 *         given domains
	 */
	private boolean typeCheck(Comparable[] t) {
		// T O B E I M P L E M E N T E D

		return true;
	} // typeCheck

	/************************************************************************************
	 * Find the classes in the "java.lang" package with given names.
	 *
	 * @param className the array of class name (e.g., {"Integer", "String"})
	 * @return an array of Java classes
	 */
	private static Class[] findClass(String[] className) {
		var classArray = new Class[className.length];

		for (var i = 0; i < className.length; i++) {
			try {
				classArray[i] = Class.forName("java.lang." + className[i]);
			} catch (ClassNotFoundException ex) {
				out.println("findClass: " + ex);
			} // try
		} // for

		return classArray;
	} // findClass

	/************************************************************************************
	 * Extract the corresponding domains.
	 *
	 * @param colPos the column positions to extract.
	 * @param group  where to extract from
	 * @return the extracted domains
	 */
	private Class[] extractDom(int[] colPos, Class[] group) {
		var obj = new Class[colPos.length];

		for (var j = 0; j < colPos.length; j++) {
			obj[j] = group[colPos[j]];
		} // for

		return obj;
	} // extractDom

} // Table class