import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sql_schemas import User
from schemas import UserCreate
from user_database import Base
from crud import create_user, delete_user
import crud

# Set up an in-memory SQLite database for testing
TEST_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create the database schema
Base.metadata.create_all(bind=engine)


@pytest.fixture
def db_session():
    """
    Fixture to provide a session for testing.
    """
    session = TestingSessionLocal()
    yield session
    session.close()


def test_crud_create_user(db_session):
    """
    Test creating a user through the crud module.
    """
    user_data = UserCreate(
        username="cruduser", 
        email="crud@example.com", 
        full_name="Crud User", 
        password="password123"
    )
    
    db_user = crud.create_user(db_session, user_data)
    
    assert db_user.username == "cruduser"
    assert db_user.email == "crud@example.com"
    assert db_user.full_name == "Crud User"
    assert db_user.disabled is False


def test_crud_delete_user(db_session):
    """
    Test deleting a user through the crud module.
    """
    # First create a user
    user_data = UserCreate(
        username="deleteuser", 
        email="delete@example.com", 
        full_name="Delete User", 
        password="password123"
    )
    crud.create_user(db_session, user_data)
    
    # Verify user exists
    user = crud.get_user_by_username(db_session, "deleteuser")
    assert user is not None
    
    # Delete the user
    result = crud.delete_user(db_session, "deleteuser")
    assert result is True
    
    # Verify user is deleted
    user = crud.get_user_by_username(db_session, "deleteuser")
    assert user is None


def test_crud_delete_nonexistent_user(db_session):
    """
    Test deleting a nonexistent user through the crud module.
    """
    result = crud.delete_user(db_session, "nonexistentuser")
    assert result is False