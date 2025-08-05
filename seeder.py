from user_database import engine, Base, SessionLocal
from crud import create_user, get_user_by_username
from dotenv import load_dotenv
import os
from schemas import UserCreate

# Load environment variables from the .env file
load_dotenv('.env')

# Create database tables
Base.metadata.create_all(bind=engine)

# Create a database session
db = SessionLocal()

# Retrieve admin credentials from the .env file
ADMIN_ID = os.getenv('ADMIN_ID')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD')

# Check if the admin user already exists
if not get_user_by_username(db, ADMIN_ID):
    # Create a default admin user
    admin_user = create_user(db, UserCreate(
        username=ADMIN_ID,
        email="admin@example.com",
        full_name="Admin",
        password=ADMIN_PASSWORD
    ))
    db.commit()
    print("Default admin user successfully created!")
else:
    print("Admin user already exists!")
