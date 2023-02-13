package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.util.List;

public class UserDaoHibernateImpl implements UserDao {
    private final SessionFactory sessionFactory = Util.getSessionFactory();
    private Transaction transaction;

    private final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS `users` (\n" +
            "  `id` INT NOT NULL AUTO_INCREMENT,\n" +
            "  `name` VARCHAR(45) NULL,\n" +
            "  `lastName` VARCHAR(45) NULL,\n" +
            "  `age` INT NULL,\n" +
            "        PRIMARY KEY (`id`))";
    private final static String DROP = "DROP TABLE IF EXISTS users;";
    public UserDaoHibernateImpl() {

    }


    @Override
    public void createUsersTable() {
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            session.createSQLQuery(CREATE_TABLE).executeUpdate();
            transaction.commit();
            System.out.println("Create table");
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            System.out.println("Error, create table:");
            e.printStackTrace();
        }
    }

    @Override
    public void dropUsersTable() {
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            session.createSQLQuery(DROP).executeUpdate();
            transaction.commit();
            System.out.println("Drop table");
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            System.out.println("Error, drop table");
            e.printStackTrace();
        }

    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            session.save(new User(name, lastName, age));
            transaction.commit();
            System.out.println("User added");
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            System.out.println("Error, user added");
            e.printStackTrace();
        }
    }

    @Override
    public void removeUserById(long id) {
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            session.delete(session.get(User.class, id));
            transaction.commit();
            System.out.println("User for id delete");
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            System.out.println("Error, delete user");
            e.printStackTrace();
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> list = null;
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            list = session.createQuery("from User", User.class).list();
            System.out.println("Пользователя выведены в лист");
        } catch (Exception e) {
            System.out.println("Ошибка выведения пользователей в лист:");
            e.printStackTrace();
        }
        return list;
    }

    @Override
    public void cleanUsersTable() {
        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.beginTransaction();
            session.createQuery("delete User ").executeUpdate();
            transaction.commit();
            System.out.println("Таблица очищена");
        } catch (HibernateException e) {
            if (transaction != null) {
                transaction.rollback();
            }
            System.out.println("Ошибка очистки таблицы");
            e.printStackTrace();
        }
    }
}
