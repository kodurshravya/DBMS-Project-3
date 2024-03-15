
/*****************************************************************************************
 * @file  StudentDB.java
 *
 * @author   Shravya Reddy Kodur
 */

import static java.lang.System.out;

/*****************************************************************************************
 * The StudentDB class makes a Student Database.
 */
class StudentDB {
	/*************************************************************************************
	 * Main method for creating, populating a Student Database.
	 * 
	 * @param args the command-line arguments
	 */
	public static void main(String[] args) {
		out.println();
		var student = new Table("Student", "id name address status", "Integer String String String", "id");

		var professor = new Table("Professor", "id name deptId", "Integer String String", "id");

		var course = new Table("Course", "crsCode deptId crsName descr", "String String String String", "crsCode");

		var teaching = new Table("Teaching", "crsCode semester profId", "String String Integer", "crcCode semester");

		var transcript = new Table("Transcript", "id name deptId", "Integer String String", "id");
		
		student = student.load("Student");
		professor = professor.load("Professor");
		course = course.load("Course");
		teaching = teaching.load("Teaching");
		transcript = transcript.load("Transcript");

	}
}