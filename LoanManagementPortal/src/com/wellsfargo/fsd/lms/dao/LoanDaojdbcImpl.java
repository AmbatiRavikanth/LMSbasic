package com.wellsfargo.fsd.lms.dao;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.Session;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.ClientInfoProvider;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.cj.jdbc.ha.LoadBalancedConnection;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.wellsfargo.fsd.lms.entity.Loan;
import com.wellsfargo.fsd.lms.exception.LoanException;

public class LoanDaojdbcImpl implements LoanDao {
	
	public static final String INS_LN_QRY="Insert into loans(loanId,p,r,emis,dob,status) values (?,?,?,?,?,?)";
	public static final String UPD_LN_QRY="update loans set p=?,r=?,emis=?,dob=?,status=? where loanId=?";
	public static final String DEL_LN_QRY="Delete from loans where loanId=?";
	public static final String GET_BY_ID_LN_QRY="select loanId,p,r,emis,dob,status from loans where loanId=?";
	public static final String GET_ALL_LNS_QRY="select loanId,p,r,emis,dob,status from loans";
	
	public Loan add(Loan loan) throws LoanException {
		if(loan!=null)
		{
			try(Connection con=ConnectionFactory.getConnection();) //try with resources makes the connection closed once the try block is completed
			{											//else try needs to be closed explicitly
														//introduced in jdk 1.8
			PreparedStatement pst =con.prepareStatement(INS_LN_QRY);
			pst.setInt(1, loan.getLoanId());
			pst.setDouble(2, loan.getPrincipal());
			pst.setDouble(3, loan.getRateOfInterest());
			pst.setInt(4, loan.getEmiCount());
			pst.setDate(5, Date.valueOf(loan.getDateOfDisbursement()));
			pst.setString(6, loan.getStatus());
			pst.executeUpdate();
			}
			catch(SQLException exp)
			{
				throw new LoanException("An error occured, Could not add the loan details!");
			}
		}
		return loan;
	}

	public Loan save(Loan loan) throws LoanException {
		if(loan!=null)
		{
			try(Connection con=ConnectionFactory.getConnection();) //try with resources makes the connection closed once the try block is completed
			{											//else try needs to be closed explicitly
														//introduced in jdk 1.8
			PreparedStatement pst =con.prepareStatement(UPD_LN_QRY);
			
			pst.setDouble(1, loan.getPrincipal());
			pst.setDouble(2, loan.getRateOfInterest());
			pst.setInt(3, loan.getEmiCount());
			pst.setDate(4, Date.valueOf(loan.getDateOfDisbursement()));
			pst.setString(5, loan.getStatus());
			pst.setInt(6, loan.getLoanId());
			pst.executeUpdate();
			}
			catch(SQLException exp)
			{
				throw new LoanException("An error occured, Could not add the loan details!");
			}
		}
		return loan;
	}

	public boolean deleteById(int loandId) throws LoanException {
		boolean isDeleted=false;
		
		try(Connection con=ConnectionFactory.getConnection();) //try with resources makes the connection closed once the try block is completed
		{											//else try needs to be closed explicitly
													//introduced in jdk 1.8
		PreparedStatement pst =con.prepareStatement(DEL_LN_QRY);
		
		
		pst.setInt(1, loandId);
		int rowsCount=pst.executeUpdate();
		isDeleted=rowsCount>0;
		}
		catch(SQLException exp)
		{
			throw new LoanException("An error occured, Could not delete the loan details!");
		}
		return isDeleted;
	}

	public List<Loan> getAll() throws LoanException {
		// TODO Auto-generated method stub
		List<Loan> loans=new ArrayList<>();
		
		try(Connection con=ConnectionFactory.getConnection();) //try with resources makes the connection closed once the try block is completed
		{											//else try needs to be closed explicitly
													//introduced in jdk 1.8
		PreparedStatement pst =con.prepareStatement(GET_ALL_LNS_QRY);
		
		ResultSet rs=pst.executeQuery();
		while(rs.next())
		{
			Loan loan=new Loan();
			loan.setLoanId(rs.getInt(1));
			loan.setPrincipal(rs.getDouble(2));
			
			loan.setRateOfInterest(rs.getDouble(3));
			loan.setEmiCount(rs.getInt(4));
			Date d=rs.getDate(5);
			loan.setDateOfDisbursement(d==null?null:d.toLocalDate());
			loan.setStatus(rs.getString(6));
			loans.add(loan);
		}
		if (loans.isEmpty()) {
			loans=null;
		}
		
		}
		catch(SQLException exp)
		{
			throw new LoanException("An error occured, Could not retrieve the loan details!");
		}
		
		return loans;
	}

	public Loan getById(int loanId) throws LoanException {
		Loan loan=null;
		
		try(Connection con=ConnectionFactory.getConnection();) //try with resources makes the connection closed once the try block is completed
		{											//else try needs to be closed explicitly
													//introduced in jdk 1.8
		PreparedStatement pst =con.prepareStatement(GET_BY_ID_LN_QRY);
		pst.setInt(1,loanId);
		ResultSet rs=pst.executeQuery();
		if(rs.next())
		{
			loan=new Loan();
			loan.setLoanId(rs.getInt(1));
			loan.setPrincipal(rs.getDouble(2));
			
			loan.setRateOfInterest(rs.getDouble(3));
			loan.setEmiCount(rs.getInt(4));
			Date d=rs.getDate(5);
			loan.setDateOfDisbursement(d==null?null:d.toLocalDate());
			loan.setStatus(rs.getString(6));			
		}		
		
		}
		catch(SQLException exp)
		{
			throw new LoanException("An error occured, Could not retrieve the loan details!");
		}
		
		return loan;
	}

}