import org.apache.spark.sql.functions.{avg, count, expr, floor, max, min, sum, when}
import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import scala.io.StdIn.readLine
import scala.io.Source
import java.io.FileNotFoundException
import java.security.MessageDigest
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object App {

	var DF: DataFrame = null;
	def getEncryptedPassword(password: String): String = {
		MessageDigest
			.getInstance("SHA-256")
			.digest(password.getBytes("UTF-8"))
			.map("%02X".format(_))
			.mkString
	}

	def pollForInput(query: String = ">Enter your Input"): String = {
		val input = readLine(s"$query\n")
		input
	}

	def findUser() {
		val name = pollForInput("Name: ")

		Database.GetUser(name) match {
			case Some(res) => {
				res.PrintInformation()
			}
			case None => {
				println(s"Couldn't find user ${name}")
			}
		}
	}

	def registerUser(): Option[User] = {
		// could conslidate these two
		val name = pollForInput("Name: ")
		val pass = getEncryptedPassword(pollForInput("Password: "))

		var new_user = new User(name, pass)
		try {
			Database.GetUser(name) match {
				case Some(res) => { println("User already exists!\n"); None }
				case None => {
					Database.InsertNewUser(new_user)
					State.SetUser(new_user)
					Some(new_user)
				}
			}
		} catch {
			case e: SQLException => { println("Not saving this session."); State.SetSave(false); None }
		}
	}

	def login() {
		val name = pollForInput("Name: ")
		val pass = getEncryptedPassword(pollForInput("Password: "))

		Database.GetUser(name) match {
			case Some(res) => { 
				if (!(res.GetPassword() == pass && res.GetUsername() == name)) { println("Incorrect login\n"); return }

				State.SetLoggedIn(true)
				State.SetUser(res)
			}
			case None => {
				println(s"Incorrect login\n")
			}
		}
	}

	def exit() {
		println("Exiting...")
		val user = State.GetUser()
		if(State.GetSave() && user.GetID() != -1) {
			Database.SaveUser(user)
		}
		State.SetLoggedIn(false)
		State.SetContinue(false)
	}

	def makeLoggedInAdmin(): Unit = {
		val user = State.GetUser()
		if (user.GetAdmin()) {
			println("You are already admin")
			return
		}

		user.SetAdmin(true)
		println(s"Granted ${user.GetUsername()} admin privileges")
	}

	def connectToDB() = {
		val configs = loadFile("resources/login.txt") 

		val driver = "com.mysql.cj.jdbc.Driver"
		val url = s"jdbc:mysql://${configs("ip")}/${configs("db")}"
		val username = s"${configs("username")}"
		val password = s"${configs("password")}"

		Class.forName(driver)
		Database.SetConnection(url, username, password)
	}

	def loginScreen(): Boolean = {
		println("\nWelcome to Scala Slots. Please log in or create a new user if you're new")
		println("[1]: Log In")
		println("[2]: Register New User")
		println("[3]: Find User")
		println("[4]: Exit")
		val input = pollForInput()
		input match {
			case "1" => { login() }
			case "2" => { registerUser() }
			case "3" => { findUser() }
			case "4" => { exit() }
			case _ => println("Invalid choice\n")
		}

		return false
	}

	def isAdmin(): Boolean = { State.GetLoggedIn() && State.GetUser().GetAdmin() }

	def printAllUsers(): Unit = {
		Database
			.GetAllUsers()
			.foreach(_.PrintInformation())
	}
	def adminMenu(): Boolean = {
		if (!isAdmin()) {
			println("You are not an admin.")
			return false
		}
		println("Administration Menu")
		println("[1]: Get All Users Information")
		println("[2]: Return")

		pollForInput() match {
			case "1" => { printAllUsers() }
			case "2" => { println("Returning to menu.\n") }
			case _ => { println("idk")}
		}

		false
	}

	def runQueries(): Unit = {
		val spark = SparkSession
			.builder
			.appName("hive test")
			.config("spark.sql.warehouse.dir", "C:\\spark\\spark-warehouse")
			.config("spark.master", "local")
			.enableHiveSupport()
			.getOrCreate()

		import spark.implicits._
		import spark.sql

		spark.sparkContext.setLogLevel("ERROR")
		sql("drop table if exists churn")
		sql("create table if not exists churn(RowNumber int, CustomerId int, Surname varchar(255), CreditScore int, Geography varchar(255), Gender varchar(255), Age int, Tenure int, Balance decimal, NumOfProducts int, HasCrCard int, IsActiveMember int, EstimatedSalary decimal, Exited int) row format delimited fields terminated by ','")
		sql("LOAD DATA LOCAL INPATH 'input/Churn_Modelling.csv' INTO TABLE churn")
		this.DF = sql("select * from churn")
		this.DF.cache()

		println("\nAverage Salary: ")
		this.DF.agg(avg($"EstimatedSalary")).show()
		//		2. average salary by gender
		println("\nAverage Salary By Gender:")
		this.DF.groupBy($"Gender").avg("EstimatedSalary").as("Average Salary").show()
		//		3. % of active employees that have been there longer than 3 years
		println("\n% of Active Senior Employees (Tenure over 3 years):")
		this.DF.filter($"Exited" === 0).agg(count(when($"Tenure" >= 3, 1)).as("tenure_over_3"), count($"Tenure").as("total")).withColumn("senior_pct", expr("100 * tenure_over_3 / total")).select($"senior_pct".as("% Senior Employees (Over 3 Years)")).show()
		//		4. % Active employees (haven't left)
		this.DF.agg(sum($"Exited").as("C1"), count($"Exited").as("C2")).withColumn("% Active Workers", expr("100 * C1 / C2")).select("% Active Workers").show()
		println("\nShortest and Longest Tenure of Ex-employees:")
		//		5. shortest, longest tenure by those whomst have left
		this.DF.filter($"Exited" === 1).agg(min($"Tenure").as("Shortest"), max($"Tenure").as("Longest")).show()

		//		6. average credit score/salary by age
		println("\nAverage Credit Score and Salary by Age Division (age / 15):")
		this.DF.withColumn("age_bucket", floor(($"Age" / 15)) * 15).groupBy($"age_bucket".as("age")).avg("CreditScore", "EstimatedSalary").select($"age", $"avg(CreditScore)".as("Average Credit Score"), $"avg(EstimatedSalary)".as("Average Salary")).orderBy("age").show()

		//		FUTURE: how long they stay before they leave?
		println("\nExpected Tenure by Age:")
		this.DF.filter($"Exited" === 1).withColumn("age_bucket", floor(($"Age" / 5)) * 5).groupBy($"age_bucket".as("age")).avg("Tenure").select($"age", $"avg(Tenure)".as("Average Tenure Before Leaving")).orderBy("age").show()
	}
	def mainMenu(): Boolean = {
		println("What would you like to do?")
		if (isAdmin()) {
			println("[a]: Admin Menu")
		}
		println("[1]: Display My Information")
		println("[2]: Make me admin")
		println("[q]: Run Spark Queries")
		println("[3]: Exit")
		val input = pollForInput()
		input match {
			case "1" => { State.GetUser().PrintInformation() }
			case "2" => { makeLoggedInAdmin() }
			case "q" => { runQueries() }
			case "3" => { exit() }
			case "a" => { adminMenu() }
			case _ => println("Invalid choice\n")
		}

		false
	}

	def loadFile(file: String): Map[String, String] = {
		try {
			Source.fromFile(file)
				.getLines
				.map(line => (line.split("=").map(word => word.trim)))
				.map({case Array(first, second) => (first, second)})
				.toMap
		} catch {
			case e: FileNotFoundException => {
				println(s"Couldn't find that file: $file\n")
				println(s"********** Place SQL login details in resource/login.txt **********")
				println(s"********** With format                                   **********")
				println(s"********** ip = your_ip:port                             **********")
				println(s"********** username = your_username                      **********")
				println(s"********** password = your_password                      **********")
				println(s"********** db = your_db                                  **********")
				
				throw e
			}
			case e: Throwable => throw e
		}
	}



	def main(args: Array[String]) {
		println("Starting....\n\n")

//		println("connecting to mysql")
		connectToDB()
//		println("connected to mysql")

		System.setSecurityManager(null)
		System.setProperty("hadoop.home.dir", "C:\\hadoop")

		 do {
		 	val input = State.GetLoggedIn() match {
		 		case true => mainMenu()
		 		case false => loginScreen()
		 	}
		 } while(State.GetContinue())

//		TODO: figure out how to hook up regular sql for user login

	}

}