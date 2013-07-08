package org.springframework.jdbc.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;
import org.springframework.util.Assert;

/**
 * Helper class that efficiently creates multiple {@link PreparedStatementCreator}
 * objects with different parameters based on a SQL statement and a single
 * set of parameter declarations.
 *
 * @author Rod Johnson
 * @author Thomas Risberg
 * @author Juergen Hoeller
 */
public class PreparedStatementCreatorFactory {

	/** The SQL, which won't change when the parameters change 
	 * 如果是空，则说明工厂只是为了创建setter*/
	private final String sql;

	/** List of SqlParameter objects. May not be <code>null</code>. 
	 * 必须和调用数值一一对应*/
	private final List<SqlParameter> declaredParameters;

	private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;

	private boolean updatableResults = false;

	private boolean returnGeneratedKeys = false;

	private String[] generatedKeysColumnNames = null;

	private NativeJdbcExtractor nativeJdbcExtractor;


	/**
	 * Create a new factory. Will need to add parameters via the
	 * {@link #addParameter} method or have no parameters.
	 */
	public PreparedStatementCreatorFactory(String sql) {
		this.sql = sql;
		this.declaredParameters = new LinkedList<SqlParameter>();
	}

	/**
	 * Create a new factory with the given DBC types.
	 * 如果这个工厂只是为了创建Setter，则sql没有用处
	 * @param types int array of JDBC types
	 * Added By George
	 */
	public PreparedStatementCreatorFactory(int[] types) {
		this.sql = null;
		this.declaredParameters = SqlParameter.sqlTypesToAnonymousParameterList(types);
	}
	
	/**
	 * Create a new factory with the given SQL and JDBC types.
	 * @param sql SQL to execute
	 * @param types int array of JDBC types
	 */
	public PreparedStatementCreatorFactory(String sql, int[] types) {
		this.sql = sql;
		this.declaredParameters = SqlParameter.sqlTypesToAnonymousParameterList(types);
	}

	/**
	 * Create a new factory with the given SQL and parameters.
	 * @param sql SQL
	 * @param declaredParameters list of {@link SqlParameter} objects
	 * @see SqlParameter
	 */
	public PreparedStatementCreatorFactory(String sql, List<SqlParameter> declaredParameters) {
		this.sql = sql;
		this.declaredParameters = declaredParameters;
	}


	/**
	 * Add a new declared parameter.
	 * <p>Order of parameter addition is significant.
	 * @param param the parameter to add to the list of declared parameters
	 */
	public void addParameter(SqlParameter param) {
		this.declaredParameters.add(param);
	}

	/**
	 * Set whether to use prepared statements that return a specific type of ResultSet.
	 * @param resultSetType the ResultSet type
	 * @see java.sql.ResultSet#TYPE_FORWARD_ONLY
	 * @see java.sql.ResultSet#TYPE_SCROLL_INSENSITIVE
	 * @see java.sql.ResultSet#TYPE_SCROLL_SENSITIVE
	 */
	public void setResultSetType(int resultSetType) {
		this.resultSetType = resultSetType;
	}

	/**
	 * Set whether to use prepared statements capable of returning updatable ResultSets.
	 */
	public void setUpdatableResults(boolean updatableResults) {
		this.updatableResults = updatableResults;
	}

	/**
	 * Set whether prepared statements should be capable of returning auto-generated keys.
	 */
	public void setReturnGeneratedKeys(boolean returnGeneratedKeys) {
		this.returnGeneratedKeys = returnGeneratedKeys;
	}

	/**
	 * Set the column names of the auto-generated keys.
	 */
	public void setGeneratedKeysColumnNames(String[] names) {
		this.generatedKeysColumnNames = names;
	}

	/**
	 * Specify the NativeJdbcExtractor to use for unwrapping PreparedStatements, if any.
	 */
	public void setNativeJdbcExtractor(NativeJdbcExtractor nativeJdbcExtractor) {
		this.nativeJdbcExtractor = nativeJdbcExtractor;
	}


	/**
	 * Return a new PreparedStatementSetter for the given parameters.
	 * @param params list of parameters (may be <code>null</code>)
	 */
	public PreparedStatementSetter newPreparedStatementSetter(List<Object> params) {
		return new PreparedStatementCreatorImpl(params != null ? params : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementSetter for the given parameters.
	 * @param params the parameter array (may be <code>null</code>)
	 */
	public PreparedStatementSetter newPreparedStatementSetter(Object[] params) {
		return new PreparedStatementCreatorImpl(params != null ? Arrays.asList(params) : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementCreator for the given parameters.
	 * @param params list of parameters (may be <code>null</code>)
	 */
	public PreparedStatementCreator newPreparedStatementCreator(List<Object> params) {
		return new PreparedStatementCreatorImpl(params != null ? params : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementCreator for the given parameters.
	 * @param params the parameter array (may be <code>null</code>)
	 */
	public PreparedStatementCreator newPreparedStatementCreator(Object[] params) {
		return new PreparedStatementCreatorImpl(params != null ? Arrays.asList(params) : Collections.emptyList());
	}

	/**
	 * Return a new PreparedStatementCreator for the given parameters.
	 * @param sqlToUse the actual SQL statement to use (if different from
	 * the factory's, for example because of named parameter expanding)
	 * @param params the parameter array (may be <code>null</code>)
	 */
	public PreparedStatementCreator newPreparedStatementCreator(String sqlToUse, List<SqlParameter> parametersToUse, Object[] params) {
		return new PreparedStatementCreatorImpl(
				sqlToUse, parametersToUse, params != null ? Arrays.asList(params) : Collections.emptyList());
	}

	public PreparedStatementCreator newPreparedStatementCreator(String sqlToUse, List<SqlParameter> parametersToUse, List<Object> params) {
		return new PreparedStatementCreatorImpl(sqlToUse, parametersToUse, params);
	}
	
	/**
	 * PreparedStatementCreator implementation returned by this class.
	 */
	private class PreparedStatementCreatorImpl
			implements PreparedStatementCreator, PreparedStatementSetter, SqlProvider, ParameterDisposer {
		private final String actualSql;
		private final List<?> values;
		private final List<SqlParameter> actualParameters;

		public PreparedStatementCreatorImpl(List<Object> parameters) {
			//UserCase[A].6，继续跟踪
			this(sql, declaredParameters, parameters);
		}
		
		/** 
		 * PreparedStatementCreator的三要素：Sql、参数类型、参数数值。
		 **/
		public PreparedStatementCreatorImpl(String actualSql, List<SqlParameter> parametersToUse, List<Object> values) {
			this.actualSql = actualSql;
			this.actualParameters = parametersToUse;
			this.values = values;
			
			Assert.notNull(values, "Parameters List must not be null");
		
			if(parametersToUse.size()<values.size()){
				throw new InvalidDataAccessApiUsageException(
						"SQL [" + actualSql + "]: given " + values.size() +
						" parameters but expected " + parametersToUse.size());
			}
			
			if (this.actualParameters.size() != declaredParameters.size()) {
				Set<String> names = new HashSet<String>();
				for (int i = 0; i < actualParameters.size(); i++) {
					String name = actualParameters.get(i).getName();
					names.add(name!=null ? name : "Parameter #" + i);
				}
				if (names.size() != declaredParameters.size()) {
					throw new InvalidDataAccessApiUsageException(
							"SQL [" + actualSql + "]: given " + names.size() +
							" parameters but expected " + declaredParameters.size());
				}
			}
			
		}

		public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
			PreparedStatement ps = null;
			if (generatedKeysColumnNames != null || returnGeneratedKeys) {
				try {
					/*
					 * 要获得自动生成的KEY，a)手工设置index b)手工设置colname c)RETURN_GENERATED_KEYS
					 */
					if (generatedKeysColumnNames != null) {
						ps = con.prepareStatement(actualSql, generatedKeysColumnNames);
					}
					else {
						ps = con.prepareStatement(actualSql, PreparedStatement.RETURN_GENERATED_KEYS);
					}
				}
				catch (AbstractMethodError ex) {
					throw new InvalidDataAccessResourceUsageException(
							"The JDBC driver is not compliant to JDBC 3.0 and thus " +
							"does not support retrieval of auto-generated keys", ex);
				}
			}
			else if (resultSetType == ResultSet.TYPE_FORWARD_ONLY && !updatableResults) {
				ps = con.prepareStatement(actualSql);
			}
			else {
				ps = con.prepareStatement(actualSql, resultSetType,
					updatableResults ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY);
			}
			
			setValues(ps);
			
			return ps;
		}

		public void setValues(PreparedStatement ps) throws SQLException {
			// Determine PreparedStatement to pass to custom types.
			PreparedStatement psToUse = ps;
			if (nativeJdbcExtractor != null) {
				psToUse = nativeJdbcExtractor.getNativePreparedStatement(ps);
			}

			// Set arguments: Does nothing if there are no parameters.
			int sqlColIndx = 1;
			//全部的参数
			for (int i = 0; i < this.values.size(); i++) {
				Object in = this.values.get(i);
				SqlParameter declaredParameter = actualParameters.get(i);
				
				if (in instanceof SqlParameterValue) {
					declaredParameter = (SqlParameter)in;
					in = ((SqlParameterValue)in).getValue();
				}
					
				//第一步，去找每个参数对应的declaredParameter
				if (in instanceof Collection && declaredParameter.getSqlType() != Types.ARRAY) {
					//这里处理类似select id, name, state from table where (name, age) in (('John', 35), ('Ann', 50))
					//的事情，参考NamedParameterUtils.substituteNamedParameters
					Collection<?> entries = (Collection<?>) in;
					for (Object entry : entries) {
						if (entry instanceof Object[]) {
							Object[] valueArray = ((Object[])entry);
							for (Object argValue : valueArray) {
								StatementCreatorUtils.setParameterValue(psToUse, sqlColIndx++, declaredParameter, argValue);
							}
						}
						else {
							StatementCreatorUtils.setParameterValue(psToUse, sqlColIndx++, declaredParameter, entry);
						}
					}
				}
				else {
					StatementCreatorUtils.setParameterValue(psToUse, sqlColIndx++, declaredParameter, in);
				}
			}
		}


		public String getSql() {
			return actualSql;
		}

		public void cleanupParameters() {
			StatementCreatorUtils.cleanupParameters(this.values);
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("PreparedStatementCreatorFactory.PreparedStatementCreatorImpl: sql=[");
			sb.append(sql).append("]; parameters=").append(this.values);
			return sb.toString();
		}
	}

}
