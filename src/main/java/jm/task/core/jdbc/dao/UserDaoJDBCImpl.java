package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private final Connection connection = Util.getConnection();

    private final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS `users` (\n" +
            "  `id` INT NOT NULL AUTO_INCREMENT,\n" +
            "  `name` VARCHAR(45) NULL,\n" +
            "  `lastName` VARCHAR(45) NULL,\n" +
            "  `age` INT NULL,\n" +
            "        PRIMARY KEY (`id`))";
    private final static String DROP = "DROP TABLE IF EXISTS users;";
    private final static String ADD = "INSERT INTO users (name, lastName, age) VALUES (?, ?, ?)";
    private final static String REMOVE_USER = "DELETE FROM users WHERE id = ?";
    private final static String SELECT = "SELECT * FROM users";
    private final static String DELETE = "DELETE FROM users";
    public UserDaoJDBCImpl() {

    }


    @Override
    public void createUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(CREATE_TABLE)) {
            connection.setAutoCommit(false);
            preparedStatement.execute();
            System.out.println("Table create");
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new RuntimeException(exception);
            }
            System.out.println("Error create database" + e.getMessage());
        }
    }

    @Override
    public void dropUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(DROP)) {
            connection.setAutoCommit(false);
            preparedStatement.execute();
            System.out.println("Table delete");
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new RuntimeException(exception);
            }
            System.out.println("Error table is not delete" + e.getMessage());
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(ADD)) {
            connection.setAutoCommit(false);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);
            preparedStatement.executeUpdate();
            System.out.println("User with name – " + name + " add in database");
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new RuntimeException(exception);
            }
            System.out.println("Error add user" + e.getMessage());
        }
    }

    @Override
    public void removeUserById(long id) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(REMOVE_USER)) {
            connection.setAutoCommit(false);
            preparedStatement.setLong(1, id);
            System.out.println("User delete");
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new RuntimeException(exception);
            }
            System.out.println("Error remove user by ID" + e.getMessage());
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                int userID = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String lastName = resultSet.getString("lastName");
                byte age = Byte.parseByte(resultSet.getString("age"));
                System.out.println("User ID = " + userID);
                System.out.println("User Name = " + name);
                System.out.println("User LastName = " + lastName);
                System.out.println("User Age = " + age);
                users.add(new User(name, lastName, age));
            }
        } catch (SQLException e) {
            System.out.println("Error getAllUsers");
            return null;
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        try (PreparedStatement preparedStatement = connection.prepareStatement(DELETE)) {
            connection.setAutoCommit(false);
            preparedStatement.executeUpdate();
            System.out.println("Table users clean");
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new RuntimeException(exception);
            }
            System.out.println("Error clean table" + e.getMessage());
        }
    }
}
