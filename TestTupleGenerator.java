
/*****************************************************************************************
 * @file  TestTupleGenerator.java
 *
 * @author   Sadiq Charaniya, John Miller
 */

import static java.lang.System.out;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/*****************************************************************************************
 * This class tests the TupleGenerator on the Student Registration Database
 * defined in the Kifer, Bernstein and Lewis 2006 database textbook (see figure
 * 3.6). The primary keys (see figure 3.6) and foreign keys (see example 3.2.2)
 * are as given in the textbook.
 */
public class TestTupleGenerator {
	/*************************************************************************************
	 * The main method is the driver for TestGenerator.
	 * 
	 * @param args the command-line arguments
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		//int[] rowCounter = { 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000, 25000, 50000 };
		//int[] rowCounter = { 40000, 35000, 30000, 25000, 20000 };
		int[] rowCounter = { 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000};

		File timestamps = new File("timestamps.txt");
		FileWriter writer = new FileWriter("timestamps.txt");
		
		for (int i = 0; i < rowCounter.length; i++) {
			System.out.println("Running for tuple size: " + rowCounter[i]);
			var test = new TupleGeneratorImpl();
			long startTime;
			long endTime;
			double duration;
	
			test.addRelSchema("Student", "id name address status", "Integer String String String", "id", null);
	
			test.addRelSchema("Professor", "id name deptId", "Integer String String", "id", null);
	
			test.addRelSchema("Course", "crsCode deptId crsName descr", "Integer String String String", "crsCode", null);
	
			test.addRelSchema("Teaching", "crsCode semester profId", "Integer Integer Integer", "crcCode semester",
					new String[][] { { "profId", "Professor", "id" }, { "crsCode", "Course", "crsCode" } });
	
					test.addRelSchema ("Transcript",
					"studId crsCode semester grade",
					"Integer String String String",
					"studId crsCode semester",
					new String [][] {{ "studId", "Student", "id"},
									 { "crsCode", "Course", "crsCode" },
									 { "crsCode semester", "Teaching", "crsCode semester" }});
	
			var tables = new String[] { "Student", "Professor", "Course", "Teaching", "Transcript" };
			var tups = new int[] { rowCounter[i], 1000, 2000, 50000, rowCounter[i] };
	
			var resultTest = test.generate(tups);
			
			//Creating Student, Professor, Course, Teaching, Transcript tables
			Table student = new Table("Student", "id name address status", "Integer String String String", "id");
			Table professor = new Table("Professor", "id name deptId", "Integer String String", "id");
			Table course = new Table("Course", "crsCode deptId crsName descr", "String String String String", "crsCode");
			Table teaching = new Table("Teaching", "crsCode semester profId", "Integer Integer Integer",
					"crsCode semester");
			Table transcript = new Table("Transcript", "studId crsCode semester grade", "Integer Integer Integer String",
					"studId crsCode semester");
			long counterStudent = 0l;
			long counterProfessor = 0l;
			long counterCourse = 0l;
			long counterTeaching = 0l;
			long counterTranscript = 0l;

			for (var j = 0; j < resultTest[0].length; j++) {
				int length = resultTest[0][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[0][j].length; k++) {
					counterStudent++;
					col[k] = (resultTest[0][j][k]).toString();
				} // for
				//Creating a student tuple for insertion into student table
				Comparable[] Student = { col[0], col[1], col[2], col[3] };
				student.insert(Student);
			}
			for (var j = 0; j < resultTest[1].length; j++) {
				int length = resultTest[1][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[1][j].length; k++) {
					col[k] = (resultTest[1][j][k]).toString();
					counterProfessor++;
				} // for
				//Creating a Professor tuple for insertion into Professor table
				Comparable[] Professor = { col[0], col[1], col[2] };
				professor.insert(Professor);
			}
			for (var j = 0; j < resultTest[2].length; j++) {
				int length = resultTest[2][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[2][j].length; k++) {
					col[k] = (resultTest[2][j][k]).toString();
					counterCourse++;
				} // for
				//Creating a Course tuple for insertion into Course table
				Comparable[] Course = { col[0], col[1], col[2], col[3] };
				course.insert(Course);
			}
			for (var j = 0; j < resultTest[3].length; j++) {
				int length = resultTest[3][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[3][j].length; k++) {
					col[k] = (resultTest[3][j][k]).toString();
					counterTeaching++;
				} // for
				//Creating a Teaching tuple for insertion into Teaching table
				Comparable[] Teaching = { col[0], col[1], col[2] };
				 teaching.insert(Teaching);
			}
			for (var j = 0; j < resultTest[4].length; j++) {
				int length = resultTest[4][j].length;
				String col[] = new String[length];
				for (var k = 0; k < resultTest[4][j].length; k++) {
					col[k] = (resultTest[4][j][k]).toString();
					counterTranscript++;
				} // for
				//Creating a Transcript tuple for insertion into Transcript table
				Comparable[] Transcript = { col[0], col[1], col[2], col[3] };
				transcript.insert(Transcript);
			}

			Random ran = new Random();
			startTime = System.nanoTime();

			var selectQuery = String.valueOf(resultTest[0][ran.nextInt(tups[0])][0]);
			
			// non-indexed select
			//Table nonIndexedSelectTest = teaching.nonIndexSelect(new KeyType(selectQuery));
			//endTime = System.nanoTime();
			//duration = (double) (endTime - startTime) / 1e6;
	
			//writer.write("Non indexed Select on teaching "+ counterStudent +" "+String.valueOf(duration));
			//writer.write("\n");

			//startTime = System.nanoTime();

			// indexed select
			//Table indexedSelectTest = teaching.select(new KeyType(selectQuery));
			//endTime = System.nanoTime();
			//duration = (double) (endTime - startTime) / 1e6;
	
			//writer.write("Indexed Select on teaching "+ counterStudent +" "+String.valueOf(duration));
			//writer.write("\n");
			

			// non-indexed join
			/*startTime = System.nanoTime();
         	Table nonIndexedJoinTest = professor.equi_join("id", "profId", teaching);
            endTime = System.nanoTime();
			duration = (double) (endTime - startTime) / 1e6;
	
			writer.write(String.valueOf(duration));
			writer.write("\n");

			System.out.println("\t\t" + duration + " millisecs");*/

			//Table indexedJoinTest = transcript.i_join("studId", "id", student);
			//indexed join
			startTime = System.nanoTime();
			Table indexedJoinTest = professor.i_join("id", "ProfId", teaching);
			
			endTime = System.nanoTime();
			duration = (double) (endTime - startTime) / 1e6;
	
			writer.write(String.valueOf(duration));
			writer.write("\n");

			System.out.println("\t\t" + duration + " millisecs");

			

			/*long startTime1 = System.nanoTime();
			Table nonIndexedJoinTest2 = transcript.equi_join("studId", "id", student);
			long endTime1 = System.nanoTime();
			double duration1 = (double) (endTime1 - startTime1) / 1e6;
	
			writer.write(String.valueOf(duration1));
			writer.write("\n");

			System.out.println("\t\t" + duration1 + " millisecs");

			long startTime2 = System.nanoTime();
			Table indexedSelectTest = student.select(new KeyType(String.valueOf(resultTest[0][ran.nextInt(tups[0])][0])));
			long endTime2 = System.nanoTime();
			double duration2 = (double) (endTime2 - startTime2) / 1e6;
	
			writer.write(String.valueOf(duration2));
			writer.write("\n");

			System.out.println("\t\t" + duration2 + " millisecs");

			long startTime3 = System.nanoTime();
			Table indexedSelectTest1 = student.select(new KeyType(String.valueOf(resultTest[0][ran.nextInt(tups[0])][0])));
			long endTime3 = System.nanoTime();
			double duration3 = (double) (endTime3 - startTime3) / 1e6;
	
			writer.write(String.valueOf(duration3));
			writer.write("\n");

			System.out.println("\t\t" + duration3 + " millisecs");*/
		}
		writer.close();
	} // main

} // TestTupleGenerator