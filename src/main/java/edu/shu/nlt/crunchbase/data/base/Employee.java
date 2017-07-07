package edu.shu.nlt.crunchbase.data.base;

import java.io.PrintStream;
import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;

public class Employee implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static Employee getInstance(JSONObject jsonObject) {
		try {
			boolean isPast = !jsonObject.isNull("is_past") ? jsonObject.getString("is_past").equals("true") : false;
			String jobTitle = jsonObject.getString("title");

			Person person = Person.getInstance(jsonObject.getJSONObject("person"));

			return new Employee(person, jobTitle, isPast);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	private boolean isPast;
	private String jobTitle;

	private Person person;

	private Employee(Person person, String jobTitle, boolean isPast) {
		super();

		this.person = person;
		this.jobTitle = jobTitle;
		this.isPast = isPast;
	}

	public String getJobTitle() {
		return jobTitle;
	}

	public Person getPerson() {
		return person;
	}

	public boolean isPast() {
		return isPast;
	}

	public void printDetails(PrintStream out) {
		out.print(jobTitle + " isPast?" + isPast + " ");
		getPerson().printDetails(out);
	}
}
