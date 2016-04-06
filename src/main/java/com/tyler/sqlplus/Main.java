package com.tyler.sqlplus;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.annotation.GlobalQuery;
import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;

public class Main {

	private static final SQLPlus DB = new SQLPlus("jdbc:mysql://localhost:3306/budget", "root", "TyDaWi@timpfmys1");
	
	public static class Transaction {
		
		@Column(name = "transaction_id", key = true) public Integer key;
		@Column(name = "date_posted") public LocalDate datePosted;
		@Column(name = "memo") public String memo;
		@Column(name = "amount") public Double amount;
		@Column(name = "planned") public Boolean planned;
		@SingleRelation public Category category;
		
		@Override
		public String toString() {
			return "Transaction [key=" + key + ", datePosted=" + datePosted + " category=" + category + ", memo=" + memo + ", amount=" + amount + ", planned=" + planned + "]";
		}
	}
	
	public static class Category {

		public enum Type { INCOME, EXPENSE; }
		
		@Column(name = "category_pkey") public Integer categoryPkey;
		@Column(name = "display_string") public String displayString;
		@Column(name = "type") public Type type;
		@Column(name = "enabled") public Boolean enabled;
		
		public Category() {}
		
		@Override
		public String toString() {
			return "Category [pk=" + categoryPkey + ", display=" + displayString + ", type=" + type + ", enabled=" + enabled + "]";
		}

	}
	
	public static class User {

		@Column(name = "user_pkey", key = true) public Integer userPkey;
		@Column(name = "user_name") public String userName;
		@Column(name = "timezone_id") public String timezoneId;
		@Column(name = "monthly_surplus_target") public Double monthlySurplusTarget;
		@MultiRelation public List<Category> categories;
		
		public User() {}
		
		@Override
		public String toString() {
			return "User [pk=" + userPkey + ", userName=" + userName + ", categories=" + categories + ", timezone=" + timezoneId + ", defaultSurplusThreshold=" + monthlySurplusTarget + "]";
		}
		
	}
	
	@GlobalQuery("select site_pkey, affiliate_id, name, email from site s")
	public static class Site {
		public @Column(name = "site_pkey", key = true) int key;
		public @Column(name = "affiliate_id") String affId;
		public @Column(name = "name") String siteName;
		public @Column(name = "email") String email;
		@Override
		public String toString() {
			return "Site [key=" + key + ", affId=" + affId + ", siteName=" + siteName + ", email=" + email + "]";
		}
		
	}
	
	public static void main(String[] args) {
		
		DB.fetch(Transaction.class, "select * from transaction t join category c on t.category_pkey = c.category_pkey").forEach(System.out::println);
		
	}
	
}
