package base.pgbase.model;

import java.sql.ResultSet;

public interface CallBack {
	Object getResultObject(ResultSet rs);
}