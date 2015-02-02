
package org.willian.owb2infa.helper.infa;

import java.util.ArrayList;
import java.util.Hashtable;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;


public class INFAInputSetUnit {

	RowSet rs;
	ArrayList<Field> includeFields;
	Hashtable<Field, Object> exprLink;

	INFAInputSetUnit() {
		rs = null;
		includeFields = new ArrayList<Field>();
		exprLink = new Hashtable<Field, Object>();

	}

	public RowSet getRs() {
		return rs;
	}

	public void setRs(RowSet rs) {
		this.rs = rs;
	}

	public ArrayList<Field> getIncludeFields() {
		return includeFields;
	}

	public void setIncludeFields(ArrayList<Field> includeFields) {
		this.includeFields = includeFields;
	}

	public Hashtable<Field, Object> getExprLink() {
		return exprLink;
	}

	public void setExprLink(Hashtable<Field, Object> exprLink) {
		this.exprLink = exprLink;
	}
}
