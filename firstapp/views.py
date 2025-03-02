from django.http import HttpResponse,HttpRequest
from django.template import loader
from django.shortcuts import render,redirect
import psycopg2
from django.contrib import messages
from datetime import datetime
from django.contrib.auth.hashers import make_password,check_password
import logging
from datetime import date
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_issue_date():
    return date.today()

def home(request):
    request.session['flag'] = 0
    template = loader.get_template("home.html")
    context = {'flag' : request.session['flag']}
    return HttpResponse(template.render(context,request))

def get_db_connection():
    logger.debug("Attempting to establish a database connection...")
    return psycopg2.connect(
        dbname="22CS10009",
        user="22CS10009",
        password="22CS10009",
        host="10.145.216.117",
        port="5432"
    )

# Signup View
def signup(request):
    logger.debug("Received a request for signup.")
    
    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")
        confirm_password = request.POST.get("confirmPassword")
        user_type = request.POST.get("userType")
        user_id = request.POST.get("userId")

        logger.debug(f"Form data received - Username: {username}, UserType: {user_type}, UserID: {user_id}")

        if password != confirm_password:
            messages.error(request, "Passwords do not match!")
            logger.warning("Signup failed: Passwords do not match.")
            return render(request, "signup.html")

        # Hash the password before storing it
        hashed_password = make_password(password)
        logger.debug("Password hashed successfully.")

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            logger.debug("Database connection established.")

            # Check if user_id exists in the user_type table
            check_user_query = f"SELECT * FROM {user_type} WHERE ID = %s;"
            cur.execute(check_user_query, (user_id,))
            existing_user = cur.fetchone()

            if not existing_user:
                messages.error(request, "User ID not found in the selected user type!")
                logger.warning(f"User ID {user_id} not found in table {user_type}.")
                cur.close()
                conn.close()
                return render(request, "signup.html")

            # Check if username is already taken within the same user_type table
            check_username_query = f"SELECT * FROM {user_type} WHERE username = %s;"
            cur.execute(check_username_query, (username,))
            username_exists = cur.fetchone()

            if username_exists:
                messages.error(request, "Username already taken for this user type!")
                logger.warning(f"Username '{username}' already exists in table {user_type}.")
                cur.close()
                conn.close()
                return render(request, "signup.html")

            # Update username and password in the existing record
            update_query = f"UPDATE {user_type} SET username = %s, passwd = %s WHERE ID = %s;"
            cur.execute(update_query, (username, hashed_password, user_id))
            conn.commit()
            logger.debug(f"User {username} successfully updated in table {user_type}.")

            cur.close()
            conn.close()

            messages.success(request, "Signup successful! Your username and password have been updated.")
            return redirect("home")  # Redirect to home page

        except psycopg2.Error as e:
            messages.error(request, f"Database error: {e}")
            logger.error(f"Database error: {e}")
            return render(request, "signup.html")

    logger.debug("Rendering signup page.")
    return render(request, "signup.html")


def login(request):
    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")
        user_type = request.POST.get("userType")  # Determines the table

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # Query to check if user exists in the selected user type table
            check_user_query = f"SELECT id, passwd FROM {user_type} WHERE username = %s;"
            cur.execute(check_user_query, (username,))
            user_record = cur.fetchone()

            cur.close()
            conn.close()

            if user_record:
                stored_userid, stored_hashed_password = user_record

                # Validate password
                if check_password(password, stored_hashed_password):
                    messages.success(request, f"Welcome {stored_userid}! You are logged in.")
                    request.session["id"] = stored_userid  # Store session data
                    request.session["user_type"] = user_type  # Store user type
                    request.session['flag'] = 1
                    return redirect(f"{user_type}")  # Redirect to dashboard/homepage

                else:
                    messages.error(request, "Invalid password. Please try again.")
                    return redirect("login")
            else:
                messages.error(request, "User not found. Please check your details.")
                return redirect("login")

        except psycopg2.Error as e:
            messages.error(request, f"Database error: {e}")
            return redirect("login")

    # GET Request: Show login page
    return render(request, "login.html", {"messages": messages.get_messages(request)})


# View to display Village Dashboard with Panchayat Employee Contacts
def village_dashboard(request):
    # Initialize variables with default empty values
    records = []
    assets_records = []
    scheme_records = []
    population_records = []
    income_records = []
    expenditure_records = []

    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")

        # Fetch Panchayat Employees data
        query = """
        SELECT c.nm, STRING_AGG(CAST(cm.mobile_no AS TEXT), ', ') AS mobile_numbers, pe.job_role 
        FROM panchayat_employees AS pe 
        INNER JOIN citizens AS c ON pe.citizen_id = c.id 
        INNER JOIN citizen_mobile AS cm ON c.id = cm.citizen_id 
        GROUP BY pe.id, c.nm, pe.job_role;
        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query)
        data = cur.fetchall()
        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        # Convert data into a list of dictionaries
        column_names = ["name", "mobile_number", "designation"]
        records = [dict(zip(column_names, row)) for row in data]

        try:
            assets_query = """
            SELECT type_a, locn
            FROM assets 
            WHERE stat = 'active';
            """
            cur.execute(assets_query)
            assets_data = cur.fetchall()

            # Convert data of asset
            assets_column_names = ["type_a", "locn"]
            assets_records = [dict(zip(assets_column_names, row)) for row in assets_data]
        except Exception as e:
            logging.error(f"Error fetching assets data: {e}")
            messages.error(request, f"Error fetching assets data: {e}")

        try:
            schemes_query = """
            SELECT 
                nm, 
                eligible_age_range, 
                eligible_gender, 
                eligible_occupation,
                eligible_income,
                eligible_land_area, 
                scheme_amt 
            FROM 
                welfare_scheme;  
            """
            cur.execute(schemes_query)
            schemes_data = cur.fetchall()

            # Convert schemes data 
            schemes_column_names = ["nm", "eligible_age_range", "eligible_gender", "eligible_occupation","eligible_income","eligible_land_area", "scheme_amt"]
            scheme_records = [dict(zip(schemes_column_names, row)) for row in schemes_data]
        except Exception as e:
            logging.error(f"Error fetching schemes data: {e}")
            messages.error(request, f"Error fetching schemes data: {e}")

        try:
            population_query = """
                SELECT 
                    COUNT(*) AS total_population,
                    SUM(CASE WHEN gender = 'Male' THEN 1 ELSE 0 END) AS male_population,
                    SUM(CASE WHEN gender = 'Female' THEN 1 ELSE 0 END) AS female_population,
                    SUM(CASE WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, DOB)) <= 18 THEN 1 ELSE 0 END) AS children_population
                FROM 
                    citizens
                WHERE 
                    DOB <= CURRENT_DATE
                    AND (date_of_death >= '2025-01-01' OR date_of_death IS NULL);
            """
            cur.execute(population_query)
            population_data = cur.fetchall()

            #Convert population data
            population_column_names = ["total_population", "male_population", "female_population", "children_population"]
            population_records = [dict(zip(population_column_names, row)) for row in population_data]
        except Exception as e:
            logging.error(f"Error fetching population data: {e}")
            messages.error(request, f"Error fetching population data: {e}")

        try:
            income_query = """
            WITH tax AS (
                SELECT EXTRACT(YEAR FROM th.trnsc_date)::INT AS year,
                    SUM(th.amount_paid) AS tax
                FROM transaction_history AS th
                GROUP BY year
            ),

            scrap AS (
                SELECT EXTRACT(YEAR FROM a.demolition_date)::INT AS year,
                    SUM(a.scrap_cost) AS scrap
                FROM assets AS a
                GROUP BY year
            )

            SELECT 
                COALESCE(t.tax, 0) AS Tax_Amount,
                COALESCE(sc.scrap, 0) AS Scrap_Amount,
                (COALESCE(t.tax, 0) + COALESCE(sc.scrap, 0)) AS Net_Worth
            FROM 
                tax t
            LEFT JOIN 
                scrap sc ON t.year = sc.year
            WHERE 
                t.year = 2024;
            """
            cur.execute(income_query)
            income_data = cur.fetchall()

            # Convert tax, scrap cost, and net income data
            income_column_names = ["tax_amount", "scrap_amount", "net_income"]
            income_records = [dict(zip(income_column_names, row)) for row in income_data]
        except Exception as e:
            logging.error(f"Error fetching income data: {e}")
            messages.error(request, f"Error fetching income data: {e}")

        try:
            expenditure_query = """
            WITH years AS (
                SELECT 2024 AS year -- Directly specify the year 2024
            ),

            salaries AS ( 
                SELECT 2024 AS year, SUM(pe.salary) AS total_salaries -- Adding year to ensure it can be joined
                FROM panchayat_employees pe
                WHERE pe.stat = 'active' -- Only consider active employees
            ),

            scheme_amounts AS (
                SELECT 2024 AS year,
                    SUM(ws.scheme_amt) AS total_scheme_amount
                FROM scheme_enrollment se
                JOIN welfare_scheme ws ON se.scheme_id = ws.scheme_id
                WHERE EXTRACT(YEAR FROM se.enrollment_date) = 2024 -- Only for the year 2024
                GROUP BY year
            ),

            asset_maintenance AS (
                SELECT 2024 AS year, 
                    SUM(ae.amount_spent) AS total_asset_maintenance
                FROM assets_expenditure ae
                WHERE EXTRACT(YEAR FROM ae.spent_date) = 2024 
                    AND ae.spent_date < CURRENT_DATE -- Only consider expenditures before the current date
                GROUP BY year
            )

            SELECT 
                COALESCE(s.total_salaries, 0) AS Salaries,
                COALESCE(sa.total_scheme_amount, 0) AS Scheme_Amounts,
                COALESCE(am.total_asset_maintenance, 0) AS Asset_Maintenance
            FROM years y
            LEFT JOIN salaries s ON y.year = s.year
            LEFT JOIN scheme_amounts sa ON y.year = sa.year
            LEFT JOIN asset_maintenance am ON y.year = am.year;

            """
            cur.execute(expenditure_query)
            expenditure_data = cur.fetchall()

            # Convert salary, scheme amount, and asset maintenance data (expenditure data)
            expenditure_column_names = ["salaries", "scheme_amount", "asset_maintenance"]
            expenditure_records = [dict(zip(expenditure_column_names, row)) for row in expenditure_data]
        except Exception as e:
            logging.error(f"Error fetching expenditure data: {e}")
            messages.error(request, f"Error fetching expenditure data: {e}")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
    logging.debug("Rendering villagedashboard.html with records.")
    return render(request, "villagedashboard.html", {
        "records": records, 
        "assets_records": assets_records, 
        "scheme_records": scheme_records, 
        "population_records": population_records, 
        "income_records": income_records, 
        "expenditure_records": expenditure_records
    })

def citizens(request):
    return render(request,"citizens.html")

def panemp(request):
    try:
        # logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        # logging.debug("Database connection established.")

        # Fetch citizens data
        query = """
        SELECT id,nm,gender,dob
        FROM citizens
        WHERE date_of_death  IS  NULL;
        """
        # logging.debug(f"Executing query: {query}")
        cur.execute(query)
        c_data = cur.fetchall()
        # logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        if request.method == "POST":
            search_land_id = request.POST.get("search_land_id", "").strip()
            search_land_type = request.POST.get("search_land_type", "").strip()
            search_land_owner = request.POST.get("search_land_owner", "").strip()
        else:
            search_land_id = ""
            search_land_type = ""
            search_land_owner = ""
   
        # Fetch land-ownership data
        # Base query (always applied)
        query = """
        SELECT 
            la.ID as land_id,
            la.type_l as land_type,
            STRING_AGG(c.nm, ', ') as list_of_owners
        FROM land_acres la
        LEFT JOIN land_ownership lo ON la.ID = lo.land_id
        LEFT JOIN citizens c ON lo.citizen_id = c.ID
        WHERE lo.to_date IS NULL
        """
        query_params = []

        # Append filters only if search parameters are provided.
        if search_land_id:
            query += " AND la.ID = %s"
            query_params.append(int(search_land_id))
        
        if search_land_type:
    # For an exact match on Land Type
            query += " AND la.type_l = %s"
            query_params.append(search_land_type)
        
        if search_land_owner:
            # Filtering by Owner Name
            query += " AND c.nm ILIKE %s"
            query_params.append(f"%{search_land_owner}%")
        
        # Append GROUP BY clause
        query += " GROUP BY la.ID, la.type_l;"

        logging.debug(f"Executing query: {query} with params: {query_params}")
        
        cur.execute(query, query_params)
        l_data = cur.fetchall()


        # Fetch certificate  data
        query = """
        SELECT
            cc.citizen_id,
            ci.nm AS citizen_name,
            cc.certificate_id,
            c.type AS certificate_type,
            c.issue_date
        FROM citizen_certificate cc
        JOIN certificates c ON cc.certificate_id = c.certificate_id
        JOIN citizens ci ON cc.citizen_id = ci.ID;
        """
        # logging.debug(f"Executing query: {query}")
        cur.execute(query)
        cer_data = cur.fetchall()
        # logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        #query to taxes 
        query = """
        SELECT tax_id,citizen_id,c.nm,tax_type, total_amount, due
        FROM payment_taxes
        JOIN citizens c on  c.id=payment_taxes.citizen_id;
        """
        cur.execute(query)
        txn_data = cur.fetchall()

        #query to scheme members 
        query = """
        SELECT 
            ws.scheme_id,
            ws.nm AS scheme_name,
            STRING_AGG(c.nm, ', ') AS members
        FROM 
            welfare_scheme ws
        LEFT JOIN 
            scheme_enrollment se ON ws.scheme_id = se.scheme_id
        LEFT JOIN 
            citizens c ON se.citizen_id = c.ID
        GROUP BY 
            ws.scheme_id, ws.nm
        ORDER BY 
            ws.scheme_id;
        """
        cur.execute(query)
        sch_data = cur.fetchall()

        #query to schemes
        query = """
        SELECT scheme_id,nm 
        FROM welfare_scheme
        """
        cur.execute(query)
        wel_sch_data = cur.fetchall()

        #query to assets 
        query = """
        SELECT 
            a.ID AS asset_id,
            a.type_a AS asset_type,
            a.locn AS asset_location,
            COALESCE(SUM(ae.amount_spent), 0) AS total_expenditure,
            a.stat AS asset_status
        FROM 
            assets a
        LEFT JOIN 
            assets_expenditure ae ON a.ID = ae.assetID
        GROUP BY 
            a.ID, a.type_a, a.locn
        ORDER BY 
            a.ID;
        """
        cur.execute(query)
        ast_data = cur.fetchall()

        if request.method == "POST": 
        # --- For Households: Get search parameters from the GET request ---
            search_house_address = request.GET.get("search_house_address", "").strip()
            search_house_members = request.GET.get("search_house_members", "").strip()

        else :
            search_house_address = ""
            search_house_members = ""

        # --- Build dynamic query for households ---
        house_query = """
        SELECT
            h.ID AS household_id,
            h.addr AS address,
            h.income AS household_income,
            STRING_AGG(c.nm, ', ') AS member_names
        FROM
            households h
        LEFT JOIN
            citizens c ON h.ID = c.household_id
        GROUP BY
            h.ID, h.addr, h.income
        ORDER BY
            h.ID;

        """
        house_query_params = []
        if search_house_address:
            house_query += " AND h.addr ILIKE %s"
            house_query_params.append(f"%{search_house_address}%")
        if search_house_members:
            house_query += " AND c.nm ILIKE %s"
            house_query_params.append(f"%{search_house_members}%")
        # house_query += " GROUP BY h.ID, h.addr, h.income ORDER BY h.ID;"

        logging.debug(f"Executing Households query: {house_query} with params: {house_query_params}")
        cur.execute(house_query, house_query_params)
        house_data = cur.fetchall()
        
        query = """
        SELECT c.complaint_id, citizen_id, c.enrolled_date, c.description
        FROM complaints c;
        """
        cur.execute(query)
        complaints_data = cur.fetchall()
        
        cur.close()
        conn.close()
        # logging.debug("Database connection closed.")

        # Convert data into a list of dictionaries
        c_column_names = ["sno","id", "name", "gender","dob"]
        citizens_records = [{"sno": idx + 1, **dict(zip(c_column_names[1:], row))} for idx, row in enumerate(c_data)]

        l_column_names=["sno","id","type","list_owners"]
        land_records= [{"sno": idx + 1, **dict(zip(l_column_names[1:], row))} for idx, row in enumerate(l_data)]

        
        cer_column_names=["sno","c_id","c_name","cer_id","cer_type","issue_date"]
        cer_records=[{"sno": idx + 1, **dict(zip(cer_column_names[1:], row))} for idx, row in enumerate(cer_data)]
        
        txn_column_names=["sno","tax_id","ctzn_id","name","tax_type","total_amount","due"]
        txn_records = [{"sno": idx + 1, **dict(zip(txn_column_names[1:], row))} for idx, row in enumerate(txn_data)]
        
        wel_sch_column_names=["sno","wel_id","wel_name"]
        wel_records = [{"sno": idx + 1, **dict(zip(wel_sch_column_names[1:], row))} for idx, row in enumerate(wel_sch_data)]

        sch_column_names=["sno","sch_id","sch_name","members"]
        sch_records = [{"sno": idx + 1, **dict(zip(sch_column_names[1:], row))} for idx, row in enumerate(sch_data)]

        ast_column_names=["sno","ast_id","ast_name","loc","exp","stat"]
        ast_records = [{"sno": idx + 1, **dict(zip(ast_column_names[1:], row))} for idx, row in enumerate(ast_data)]
        
        house_column_names=["sno","house_id","addr","members"]
        house_records = [{"sno": idx + 1, **dict(zip(house_column_names[1:], row))} for idx, row in enumerate(house_data)]
        
        complaints_column_names=["sno","complaint_id","citizen_id","date","description"]
        complaints_records = [{"sno": idx + 1, **dict(zip(complaints_column_names[1:], row))} for idx, row in enumerate(complaints_data)]
        # logging.debug(f"Processed records: {records}")  # Debugging Output

        search_params = {
            "search_land_id": search_land_id,
            "search_land_type": search_land_type,
            "search_land_owner": search_land_owner,
            "search_house_address": search_house_address,
            "search_house_members": search_house_members,
        }

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return render(request,"panchayat_employees.html",{"citizens_record":citizens_records,"land_records":land_records,"search_params": search_params,"cer_records":cer_records,"tax_records":txn_records,"wel_records":wel_records,"sch_records":sch_records,"assets_records":ast_records,"house_records":house_records,"complaints_records":complaints_records})

def addcitizen(request):
    logging.debug("addcitizen view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        name = request.POST.get("nm")
        gender = request.POST.get("gender")
        household_id = request.POST.get("household_id")
        education_qualification = request.POST.get("education_qualification")
        father = request.POST.get("father") or None
        mother = request.POST.get("mother") or None
        spouse = request.POST.get("spouse") or None
        dob = request.POST.get("DOB")
        category = request.POST.get("category")
        income = request.POST.get("income")
        occupation = request.POST.get("occupation") 
        
        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            # Convert empty strings to NULL for optional fields
            household_id = int(household_id) if household_id else None
            father = int(father) if father else None
            mother = int(mother) if mother else None
            spouse = int(spouse) if spouse else None
            income = float(income) if income else None
            dob = datetime.strptime(dob, "%Y-%m-%d").date() if dob else None
            if occupation=="nan":
                occupation=None
            query = """
                INSERT INTO citizens (nm, gender, household_id, education_qualification, father, mother, spouse, DOB, category, income, occupation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            values = (name, gender, household_id, education_qualification, father, mother, spouse, dob, category, income, occupation)
            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "Citizen added successfully.")
            return redirect("panchayat_employees")  # Redirect to home or another page after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            
    return render(request, "addcitizen.html")

def addland(request):
    logging.debug("addlandn view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        area = float(request.POST.get("area_acres")) if request.POST.get("area_acres") else None
        type_l = request.POST.get("type_l")
        owner_citizen_id = request.POST.get("owner_citizen_id")
        old_id = request.POST.get("old_id") or None
        stat = "active"  # Set default status as "active"
        from_date = date.today()
        
        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            # Convert empty strings to NULL for optional fields
            old_id = int(old_id) if old_id else None

            if old_id:
                cur.execute("SELECT id FROM land_acres WHERE id = %s;", (old_id,))
                if not cur.fetchone():
                    messages.error(request, "Invalid old_id reference.")
                    return redirect("addland")
                # Update status of old land record to "deactivated"
                cur.execute("UPDATE land_acres SET stat = %s WHERE id = %s;", ("Inactive", old_id))
                
                # Update to_date in land_ownership for the old land record
                cur.execute("UPDATE land_ownership SET to_date = %s WHERE land_id = %s AND citizen_id = %s AND to_date IS NULL;", (from_date, old_id, owner_citizen_id))
            
            cur.execute("SELECT COUNT(*) FROM citizens WHERE id = %s;", (owner_citizen_id,))
            if cur.fetchone()[0] == 0:
                messages.error(request, "Owner Citizen ID does not exist.")
                return redirect("panchayat_employees")
            
            addland_query = """
                INSERT INTO land_acres (area_acres, type_l, old_id, stat)
                VALUES (%s, %s, %s, %s)
                RETURNING id;
            """
            
            add_land_values = (area, type_l, old_id, stat)
            logging.debug(f"Executing SQL Query: {addland_query} with values {add_land_values}")
            
            cur.execute(addland_query, add_land_values)
            
            id = cur.fetchone()
            if id:
                id = id[0]  # Extract the ID
            else:
                messages.error(request, "Failed to retrieve land ID.")
                return redirect("panchayat_employees")
            
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            insert_citizen_land_query = """
                INSERT INTO land_ownership (land_id, citizen_id, from_date)
                VALUES (%s, %s, %s);
            """
            
            insert_citizen_land_values = (id, owner_citizen_id, from_date)
            logging.debug(f"Executing SQL Query: {insert_citizen_land_query} with values {insert_citizen_land_values}")
            
            cur.execute(insert_citizen_land_query, insert_citizen_land_values)
            conn.commit()
            
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "land added successfully.")
            return redirect("panchayat_employees")  # Redirect to home or another page after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            
    return render(request, "addland.html")


def issuecertificate(request):
    logging.debug("issuecertificate view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        type_cer = request.POST.get("certificate_type")
        issue_date = date.today()
        event_date = request.POST.get("event_date")
        citizen_id = request.POST.get("citizen_id")


        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            cur.execute("SELECT COUNT(*) FROM citizens WHERE id = %s;", (citizen_id,))
            if cur.fetchone()[0] == 0:
                messages.error(request, "Citizen ID does not exist.")
                return redirect("panchayat_employees")

            insert_certificate_query = """
                INSERT INTO certificates (type, issue_date, event_date)
                VALUES (%s, %s, %s)
                RETURNING certificate_id;
            """
            
            cer_values = (type_cer, issue_date, event_date)
            logging.debug(f"Executing SQL Query: {insert_certificate_query} with values {cer_values}")
            
            cur.execute(insert_certificate_query, cer_values)
            certificate_id = cur.fetchone()
            if certificate_id:
                certificate_id = certificate_id[0]  # Extract the ID
            else:
                messages.error(request, "Failed to retrieve certificate ID.")
                return redirect("panchayat_employees")
            
            conn.commit()
            
            insert_citizen_certificate_query = """
                INSERT INTO citizen_certificate (citizen_id, certificate_id)
                VALUES (%s, %s);
            """
            
            cer_ctzn_values = (citizen_id, certificate_id)
            logging.debug(f"Executing SQL Query: {insert_citizen_certificate_query} with values {cer_ctzn_values}")
            
            cur.execute(insert_citizen_certificate_query, cer_ctzn_values)
            conn.commit()
            
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "certificate issued successfully.")
            return redirect("panchayat_employees")  # Redirect to home or another page after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            
    return render(request, "issuecertificate.html")

def enrolltoschemes(request):
    logging.debug("enrolltoschemes view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        scheme_id = request.POST.get("scheme_id")
        citizen_id = request.POST.get("citizen_id")
        enrollment_date = date.today()
        
        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            cur.execute("SELECT COUNT(*) FROM citizens WHERE id = %s and date_of_death is NULL;", (citizen_id,))
            if cur.fetchone()[0] == 0:
                messages.error(request, "Citizen ID does not exist or dead.")
                return redirect("panchayat_employees")
            
            cur.execute("SELECT COUNT(*) FROM welfare_scheme WHERE scheme_id = %s;", (scheme_id,))
            if cur.fetchone()[0] == 0:
                messages.error(request, "scheme ID does not exist.")
                return redirect("panchayat_employees")
            
            # **Check if citizen is already enrolled in the scheme in this year **
            cur.execute(
                """
                SELECT COUNT(*) 
                FROM scheme_enrollment 
                WHERE citizen_id = %s 
                  AND scheme_id = %s
                  AND EXTRACT(YEAR FROM enrollment_date) = EXTRACT(YEAR FROM CURRENT_DATE);
                """,
                (citizen_id, scheme_id)

            )
            if cur.fetchone()[0] > 0:
                messages.error(request, "Citizen is already enrolled in this scheme in this year.")
                return redirect("panchayat_employees")
            
            # Check eligibility
            eligibility_query = """
                SELECT 
                    c.id, c.nm, c.gender, c.occupation, c.income, 
                    EXTRACT(YEAR FROM AGE(c.dob)) AS age,
                    w.nm AS scheme_name, w.eligible_age_range, w.eligible_gender, 
                    w.eligible_occupation, w.eligible_income, w.eligible_land_area 
                FROM citizens c
                JOIN welfare_scheme w ON w.scheme_id = %s
                LEFT JOIN (
                    SELECT lo.citizen_id, COALESCE(SUM(la.area_acres), 0.00) AS total_land_area
                    FROM land_ownership lo
                    JOIN land_acres la ON lo.land_id = la.id
                    GROUP BY lo.citizen_id
                ) land_data ON land_data.citizen_id = c.id
                WHERE c.id = %s
                AND EXTRACT(YEAR FROM AGE(c.dob)) BETWEEN 
                    SPLIT_PART(w.eligible_age_range, '-', 1)::INTEGER 
                    AND SPLIT_PART(w.eligible_age_range, '-', 2)::INTEGER
                AND (w.eligible_gender = 'Any' OR w.eligible_gender = c.gender)
                AND (w.eligible_occupation = 'Any' OR w.eligible_occupation = c.occupation)
                AND (w.eligible_income = 0.00 OR c.income <= w.eligible_income)
                AND (w.eligible_land_area = 0.00 OR land_data.total_land_area <= w.eligible_land_area);
            """

            cur.execute(eligibility_query, (scheme_id, citizen_id))
            if not cur.fetchone():
                messages.error(request, "Citizen is not eligible for the selected scheme.")
                print("Citizen is not eligible for the selected scheme.")
                return redirect("panchayat_employees")
            
            query = """
                INSERT INTO scheme_enrollment (citizen_id, scheme_id, enrollment_date)
                VALUES (%s, %s, %s);
            """
            
            values = (citizen_id, scheme_id, enrollment_date)
            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "Citizen enrolled to scheme successfully.")
            return redirect("panchayat_employees")  # Redirect to home or another page after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            
    return render(request, "enrolltoschemes.html")

def addschemes(request):
    
    logging.debug("addscheme view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        name = request.POST.get("scheme_name")
        age_start = request.POST.get("eligible_age_start")
        age_end = request.POST.get("eligible_age_end")
        gender = request.POST.get("eligible_gender")
        occupation = request.POST.get("eligible_occupation")
        income = request.POST.get("eligible_income") 
        scheme_amt = request.POST.get("scheme_amt")
        land_area = float(request.POST.get("eligible_land_area")) if request.POST.get("eligible_land_area") else None

        # Convert age range to a string in the format "start-end"
        eligible_age_range = f"{age_start}-{age_end}"
        
        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            query = """
                INSERT INTO welfare_scheme (nm, eligible_age_range, eligible_gender, eligible_occupation, eligible_income, eligible_land_area, scheme_amt)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            
            values = (name, eligible_age_range, gender, occupation, income, land_area, scheme_amt)
            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "scheme added successfully.")
            return redirect("panchayat_employees")  # Redirect to home or another page after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    
    logging.debug("addscheme view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        name = request.POST.get("scheme_name")
        age_start = request.POST.get("eligible_age_start")
        age_end = request.POST.get("eligible_age_end")
        gender = request.POST.get("eligible_gender")
        occupation = request.POST.get("eligible_occupation")
        income = request.POST.get("eligible_income") 
        scheme_amt = request.POST.get("scheme_amt")
        land_area = float(request.POST.get("eligible_land_area")) if request.POST.get("eligible_land_area") else None

        # Convert age range to a string in the format "start-end"
        eligible_age_range = f"{age_start}-{age_end}"
        
        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            query = """
                INSERT INTO welfare_scheme (nm, eligible_age_range, eligible_gender, eligible_occupation, eligible_income, eligible_land_area, scheme_amt)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            
            values = (name, eligible_age_range, gender, occupation, income, land_area, scheme_amt)
            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "scheme added successfully.")
            return redirect("panchayat_employees")  # Redirect to home or another page after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    return render(request, "addschemes.html")


def addassets(request):
    
    logging.debug("addassets view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        name = request.POST.get("type_a")
        location = request.POST.get("locn")
        installation_date = request.POST.get("installation_date")
        scrap_cost = request.POST.get("scrap_cost")
        stat = "active"
        
        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            query = """
                INSERT INTO assets (type_a, locn, installation_date, stat, scrap_cost)
                VALUES (%s, %s, %s, %s, %s);
            """
            
            values = (name, location, installation_date, stat, scrap_cost)
            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "asset added successfully.")
            return redirect("panchayat_employees")  # Redirect to home or another page after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    return render(request, "addassets.html")

def addhousehold(request):
    logging.debug("addhousehold view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        addr = request.POST.get("addr")
        citizen_id = request.POST.get("citizen_id")

        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            # Check if the citizen exists
            cur.execute("SELECT COUNT(*) FROM citizens WHERE id = %s;", (citizen_id,))
            if cur.fetchone()[0] == 0:
                messages.error(request, "Citizen ID does not exist.")
                return redirect("panchayat_employees")
            
            # Insert new household
            addhouse_query = """
                INSERT INTO households (addr)
                VALUES (%s)
                RETURNING id;
            """
            
            add_house_values = (addr,)  # Ensure it's a tuple
            logging.debug(f"Executing SQL Query: {addhouse_query} with values {add_house_values}")
            
            cur.execute(addhouse_query, add_house_values)
            household_id = cur.fetchone()[0]

            if not household_id:
                messages.error(request, "Failed to retrieve household ID.")
                return redirect("panchayat_employees")
            
            conn.commit()
            logging.debug(f"Household added successfully with ID {household_id}.")

            # Assign the household to the citizen
            update_citizen_query = """
                UPDATE citizens 
                SET household_id = %s 
                WHERE id = %s;
            """
            
            update_citizen_values = (household_id, citizen_id)
            logging.debug(f"Executing SQL Query: {update_citizen_query} with values {update_citizen_values}")
            
            cur.execute(update_citizen_query, update_citizen_values)
            conn.commit()
            
            messages.success(request, "Household added successfully.")
            return redirect("panchayat_employees")

        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, "Database error. Please try again.")

        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            messages.error(request, "An unexpected error occurred. Please try again.")

        finally:
            if 'cur' in locals() and cur:
                cur.close()
            if 'conn' in locals() and conn:
                conn.close()
            logging.debug("Database connection closed.")
            
    return render(request, "addhousehold.html")

def updateCitizen(request):
    records = {}
    if request.method == "POST":
        # Get form data from POST request
        ctzn_id=request.POST.get("id")
        name = request.POST.get("name")
        gender = request.POST.get("gender")
        household_id = request.POST.get("household_id")
        educational_qualification = request.POST.get("educational_qualification")
        dob = request.POST.get("dob")  # Ensure this is in YYYY-MM-DD format
        income = request.POST.get("income")
        occupation = request.POST.get("occupation")

        logging.debug(f"Received form data: {request.POST}")

        # Validate input (ensure required fields are not empty)
        if not all([name, gender, household_id, educational_qualification, dob, income, occupation]):
            messages.error(request, "All fields are required.")
            return redirect("updateCitizen")

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established for UPDATE.")

            # Update query
            update_query = """
                UPDATE citizens 
                SET nm = %s, gender = %s, household_id = %s, education_qualification = %s, 
                    dob = %s, income = %s, occupation = %s
                WHERE id = %s;
            """
            cur.execute(update_query, (name, gender, household_id, educational_qualification, dob, income, occupation, ctzn_id))
            conn.commit()

            logging.debug(f"Database updated successfully for ID: {ctzn_id}.")
            messages.success(request, "Citizen profile updated successfully.")

            # Close DB connection
            cur.close()
            conn.close()
            logging.debug("Database connection closed after update.")

            return redirect("panchayat_employees")  # Redirect after successful update

        except psycopg2.Error as e:
            logging.error(f"Database update error: {e}")
            messages.error(request, f"Database error: {e}")

    elif request.method == "GET":
        # Get ID from URL or fall back to session ID
        id=request.GET.get("ctzn_id")    
        logging.debug(f"ID retrieved: {id} ")

        if not id:
            messages.error(request, "User ID not found in session or URL.")
            logging.error("User ID not found in session or URL.")
            return render(request, "updateCitizen.html", {"record": records})

        try:
            id = int(id)  # Ensure ID is an integer
        except ValueError:
            messages.error(request, "Invalid ID format.")
            logging.error("Invalid ID format received.")
            return render(request, "updateCitizen.html", {"record": records})

            # If GET request, fetch the existing citizen data
        try:
            logging.debug("Connecting to the database for GET request...")
            conn = get_db_connection()
            cur = conn.cursor()

            query = """
                SELECT nm, gender, household_id, education_qualification, dob, income, occupation
                FROM citizens
                WHERE id = %s;
            """
            logging.debug(f"Executing query: {query} with id: {id}")
            cur.execute(query, (id,))  
            c_data = cur.fetchone()  # Fetch a single row
            logging.debug(f"Query executed successfully. Retrieved data: {c_data}")

            if c_data:
                c_columns = ["name", "gender", "household_id", "educational_qualification", "dob", "income", "occupation"]
                records = dict(zip(c_columns, c_data))

                # Convert date format if needed
                if records["dob"]:
                    records["dob"] = records["dob"].strftime("%Y-%m-%d")  # Ensure correct format for <input type="date">

                logging.debug(f"Final records sent to template: {records}")

            cur.close()
            conn.close()
            logging.debug("Database connection closed.")

        except psycopg2.Error as e:
            logging.error(f"Database fetch error: {e}")
            messages.error(request, f"Database error: {e}")
        
        return render(request, "updateCitizen.html", {"record": records})


def viewscheme(request):
    scheme_record = {}

    if request.method == "POST":
        # Fetch form data from POST request
        scheme_id = request.POST.get("scheme_id")
        name = request.POST.get("name")
        age_range = request.POST.get("eligible_age_range")
        gender = request.POST.get("eligible_gender")
        occupation = request.POST.get("eligible_occupation")
        # income = request.POST.get("eligible_income")
        # land_area = request.POST.get("eligible_land_area")
        # scheme_amt = request.POST.get("scheme_amt")
        
        income = float(request.POST.get("eligible_income", 0))
        land_area = float(request.POST.get("eligible_land_area", 0))
        scheme_amt = float(request.POST.get("scheme_amt", 0))


        logging.debug(f"Received form data: {request.POST}")

        if not all([name, age_range, gender, occupation, income, land_area, scheme_amt]):
            messages.error(request, "All fields are required.")
            return redirect("viewscheme")

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established for UPDATE.")

            # Update query
            update_query = """
                UPDATE welfare_scheme 
                SET nm = %s, eligible_age_range = %s, eligible_gender = %s, 
                    eligible_occupation = %s, eligible_income = %s, 
                    eligible_land_area = %s, scheme_amt = %s
                WHERE scheme_id = %s;
            """
            cur.execute(update_query, (name, age_range, gender, occupation, income, land_area, scheme_amt, scheme_id))
            conn.commit()

            logging.debug(f"Database updated successfully for scheme_id: {scheme_id}.")
            messages.success(request, "Scheme details updated successfully.")

            # Close DB connection
            cur.close()
            conn.close()
            logging.debug("Database connection closed after update.")

            return redirect("panchayat_employees")  # Redirect after successful update

        except psycopg2.Error as e:
            logging.error(f"Database update error: {e}")
            messages.error(request, f"Database error: {e}")

    elif request.method == "GET":
        scheme_id = request.GET.get("wel_id")
        logging.debug(f"Scheme ID retrieved: {scheme_id}")

        if not scheme_id:
            messages.error(request, "Scheme ID not found in URL.")
            logging.error("Scheme ID not found in URL.")
            return render(request, "viewscheme.html", {"scheme": scheme_record})

        try:
            scheme_id = int(scheme_id)
        except ValueError:
            messages.error(request, "Invalid Scheme ID format.")
            logging.error("Invalid Scheme ID format received.")
            return render(request, "viewscheme.html", {"scheme": scheme_record})

        try:
            logging.debug("Connecting to the database for GET request...")
            conn = get_db_connection()
            cur = conn.cursor()

            # Fetch existing scheme data
            query = """
                SELECT nm, eligible_age_range, eligible_gender, eligible_occupation, 
                       eligible_income, eligible_land_area, scheme_amt
                FROM welfare_scheme
                WHERE scheme_id = %s;
            """
            logging.debug(f"Executing query: {query} with scheme_id: {scheme_id}")
            cur.execute(query, (scheme_id,))
            scheme_data = cur.fetchone()
            logging.debug(f"Query executed successfully. Retrieved data: {scheme_data}")

            if scheme_data:
                columns = ["name", "eligible_age_range", "eligible_gender", "eligible_occupation",
                           "eligible_income", "eligible_land_area", "scheme_amt"]
                scheme_record = dict(zip(columns, scheme_data))

            cur.close()
            conn.close()
            logging.debug("Database connection closed.")

        except psycopg2.Error as e:
            logging.error(f"Database fetch error: {e}")
            messages.error(request, f"Database error: {e}")

    return render(request, "viewscheme.html", {"scheme": scheme_record})

def delete_scheme(request, scheme_id):
    try:
        logging.debug(f"Attempting to delete scheme with ID: {scheme_id}")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Delete related enrollments first (if applicable)
        delete_enrollments_query = "DELETE FROM scheme_enrollment WHERE scheme_id = %s;"
        cur.execute(delete_enrollments_query, (scheme_id,))
        
        # Delete the scheme itself
        delete_scheme_query = "DELETE FROM welfare_scheme WHERE scheme_id = %s;"
        cur.execute(delete_scheme_query, (scheme_id,))
        
        conn.commit()  # Save changes
        logging.debug(f"Scheme with ID {scheme_id} deleted successfully.")

        cur.close()
        conn.close()
        
        messages.success(request, "Scheme deleted successfully.")
        return redirect("panchayat_employees")  # Redirect to schemes list page

    except psycopg2.Error as e:
        logging.error(f"Database delete error: {e}")
        messages.error(request, f"Error deleting scheme: {e}")
        return redirect("panchayat_employees")  # Redirect even if error occurs

def edit_asset(request):
    asset_data = {}

    if request.method == "POST":
        asset_id = request.POST.get("asset_id")
        amount_spent = request.POST.get("amount_spent")
        spent_date = request.POST.get("spent_date")

        logging.debug(f"Received form data: {request.POST}")

        if not asset_id or not amount_spent or not spent_date:
            messages.error(request, "All fields are required.")
            return redirect("edit_asset")

        try:
            amount_spent = float(amount_spent)
        except ValueError:
            messages.error(request, "Invalid expenditure amount.")
            return redirect("edit_asset")

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")

            # Insert into assets_expenditure table
            insert_query = """
                INSERT INTO assets_expenditure (assetid, amount_spent, spent_date)
                VALUES (%s, %s, %s);
            """
            cur.execute(insert_query, (asset_id, amount_spent, spent_date))
            conn.commit()

            logging.debug(f"Expenditure added for asset_id: {asset_id}.")
            messages.success(request, "Expenditure added successfully.")

            cur.close()
            conn.close()
            logging.debug("Database connection closed.")

            return redirect("panchayat_employees")  # Redirect to asset list

        except psycopg2.Error as e:
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")

    elif request.method == "GET":
        asset_id = request.GET.get("asset_id")
        logging.debug(f"Asset ID retrieved: {asset_id}")

        if not asset_id:
            messages.error(request, "Asset ID not found in URL.")
            return render(request, "edit_asset.html", {"asset": asset_data})

        try:
            asset_id = int(asset_id)
        except ValueError:
            messages.error(request, "Invalid Asset ID format.")
            return render(request, "edit_asset.html", {"asset": asset_data})

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # Fetch asset details
            query = """
                SELECT id, type_a, locn, installation_date, stat, demolition_date, scrap_cost
                FROM assets
                WHERE id = %s;
            """
            cur.execute(query, (asset_id,))
            asset_row = cur.fetchone()

            if asset_row:
                columns = ["id", "type_a", "locn", "installation_date", "stat", "demolition_date", "scrap_cost"]
                asset_data = dict(zip(columns, asset_row))

                # Convert date fields to string format
                if asset_data.get("installation_date"):
                    asset_data["installation_date"] = asset_data["installation_date"].strftime("%Y-%m-%d")

                if asset_data.get("demolition_date"):
                    asset_data["demolition_date"] = asset_data["demolition_date"].strftime("%Y-%m-%d")


            cur.close()
            conn.close()

        except psycopg2.Error as e:
            logging.error(f"Database fetch error: {e}")
            messages.error(request, f"Database error: {e}")

    return render(request, "edit_asset.html", {"asset": asset_data})

def delete_asset(request, asset_id):
    try:
        logging.debug(f"Marking asset ID {asset_id} as inactive and updating demolition date.")

        conn = get_db_connection()
        cur = conn.cursor()

        # Update `demolition_date` to today and set `stat` to "inactive"
        update_query = """
        UPDATE assets 
        SET demolition_date = %s, stat = 'inactive' 
        WHERE id = %s;
        """
        cur.execute(update_query, (date.today(), asset_id))

        conn.commit()  # Save changes
        logging.debug(f"Asset ID {asset_id} updated successfully.")

        cur.close()
        conn.close()

        messages.success(request, "Asset marked as inactive successfully.")
        return redirect("panchayat_employees")  # Redirect to the asset list page

    except psycopg2.Error as e:
        logging.error(f"Database update error: {e}")
        messages.error(request, f"Error updating asset: {e}")
        return redirect("panchayat_employees")  # Redirect even if an error occurs

def update_all_taxes(request):
    try:
        logging.debug("Updating all tax records.")

        conn = get_db_connection()
        cur = conn.cursor()
        
        # Fetch all citizens with income
        cur.execute("SELECT id, income FROM citizens")
        citizens = cur.fetchall()

        # Fetch land ownership details
        cur.execute("""
            SELECT lo.citizen_id, COALESCE(SUM(la.area_acres), 0) AS total_land_area
            FROM land_ownership lo
            JOIN land_acres la ON lo.land_id = la.id
            WHERE lo.to_date IS NULL OR lo.to_date > CURRENT_DATE
            GROUP BY lo.citizen_id
        """)
        land_ownership = dict(cur.fetchall())
        
        year = date.today().year
        
        for citizen_id, income in citizens:
            # Calculate income tax
            if income < 500000:
                income_tax = 0
            elif income > 1000000:
                income_tax = income * Decimal('0.03')
            else:
                income_tax = income * Decimal('0.01')
            
            # Fetch land area for the citizen
            land_area = land_ownership.get(citizen_id, 0)
            land_tax = max(0, (land_area - 5) * 300)
            
            # Insert income tax record if applicable
            if income_tax > 0:
                cur.execute("""
                    INSERT INTO payment_taxes (citizen_id, total_amount, due, yr, tax_type)
                    VALUES (%s, %s, %s, %s, %s)
                """, (citizen_id, income_tax, income_tax, year, 'Income Tax'))
            
            # Insert land tax record if applicable
            if land_tax > 0:
                cur.execute("""
                    INSERT INTO payment_taxes (citizen_id, total_amount, due, yr, tax_type)
                    VALUES (%s, %s, %s, %s, %s)
                """, (citizen_id, land_tax, land_tax, year, 'Land Tax'))
            
        conn.commit()
        logging.debug("All tax records updated successfully.")

        cur.close()
        conn.close()

        messages.success(request, "All tax records updated successfully.")
        return redirect("panchayat_employees")

    except psycopg2.Error as e:
        logging.error(f"Database update error: {e}")
        messages.error(request, f"Error updating tax records: {e}")
        return redirect("panchayat_employees")

def enroll_eligible_members(request):
    try:
        logging.debug("Enrolling eligible citizens in welfare schemes.")

        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch all welfare schemes and their eligibility criteria
        cur.execute("SELECT scheme_id, eligible_age_range, eligible_gender, eligible_occupation, eligible_income, eligible_land_area FROM welfare_scheme")
        schemes = cur.fetchall()

        # Fetch all citizens and their details
        cur.execute("""
            SELECT c.id, c.gender, c.occupation, c.income, c.dob, COALESCE(lo.total_land_area, 0) AS land_area
            FROM citizens c
            LEFT JOIN (
                SELECT lo.citizen_id, COALESCE(SUM(la.area_acres), 0) AS total_land_area
                FROM land_ownership lo
                JOIN land_acres la ON lo.land_id = la.id
                WHERE lo.to_date IS NULL OR lo.to_date > CURRENT_DATE
                GROUP BY lo.citizen_id
            ) lo ON c.id = lo.citizen_id
        """)
        citizens = cur.fetchall()

        year_today = date.today().year

        for citizen in citizens:
            citizen_id, gender, occupation, income, dob, land_area = citizen
            age = year_today - dob.year

            for scheme in schemes:
                scheme_id, age_range, eligible_gender, eligible_occupation, eligible_income, eligible_land = scheme

                # Parse eligible age range
                min_age, max_age = map(int, age_range.split('-'))

                # Check eligibility
                if (
                    min_age <= age <= max_age and
                    (eligible_gender == 'Any' or eligible_gender == gender) and
                    (eligible_occupation == 'Any' or eligible_occupation == occupation) and
                    (income <= eligible_income) and
                    (land_area <= eligible_land)
                ):
                    # Check if already enrolled
                    cur.execute("SELECT 1 FROM scheme_enrollment WHERE citizen_id = %s AND scheme_id = %s", (citizen_id, scheme_id))
                    if not cur.fetchone():
                        # Enroll the citizen
                        cur.execute("INSERT INTO scheme_enrollment (citizen_id, scheme_id, enrollment_date) VALUES (%s, %s, CURRENT_DATE)", (citizen_id, scheme_id))

        conn.commit()
        logging.debug("All eligible citizens enrolled successfully.")

        cur.close()
        conn.close()

        messages.success(request, "All eligible citizens enrolled successfully.")
        return redirect("panchayat_employees")

    except psycopg2.Error as e:
        logging.error(f"Database update error: {e}")
        messages.error(request, f"Error enrolling citizens: {e}")
        return redirect("panchayat_employees")



# View to fetch citizen's taxes
def citizenTaxes(request):
    logging.debug("citizenTaxes view called.")

    # **Check if the user is logged in (flag must be 1)**
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to view tax details.")
        return redirect("login")  # Redirect to login page

    # **Check if citizen_id exists in session**
    citizen_id = request.session.get("id")
    if not citizen_id:
        messages.error(request, "Invalid session. Please log in again.")
        return redirect("login")

    try:
        logging.debug(f"Fetching tax details for citizen_id: {citizen_id}")

        conn = get_db_connection()
        cur = conn.cursor()

        # **Corrected Query using parameterized SQL**
        query = """
        SELECT tax_id, tax_type, yr, total_amount, due
        FROM payment_taxes
        WHERE citizen_id = %s ;
        """
        cur.execute(query, (citizen_id,))
        data = cur.fetchall()

        cur.close()
        conn.close()

        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        # **Convert data into a list of dictionaries with S.No**
        column_names = ["s_no", "tax_id", "tax_type", "year", "total_amount", "due"]
        records = [{"s_no": idx + 1, **dict(zip(column_names[1:], row))} for idx, row in enumerate(data)]

        logging.debug(f"Processed records: {records}")

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)
        messages.error(request, error_message)
        records = []

    return render(request, "citizenTaxes.html", {"records": records})


# View to process citizen payments


def citizenPayments(request):
    if request.method == "POST":
        amount = request.POST.get("amount")
        tax_id = request.POST.get("tax_id")  # Get tax_id from POST request
        id = request.session.get("id")  # Get citizen_id from session
        type = request.session.get("user_type")

        logging.debug(f"Received Amount: {amount}, Tax ID: {tax_id},  ID: {id}")

        if not id:
            messages.error(request, "You must be logged in to make a payment.")
            logging.warning("Unauthorized access attempt: No citizen_id in session")
            return redirect("login")

        if not amount or not tax_id:
            messages.error(request, "Invalid payment details.")
            logging.warning(f"Invalid form submission: amount={amount}, tax_id={tax_id}")
            return redirect("mycertificates")

        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")

            # **SQL Transaction: Insert into transaction_history & Update payment_taxes**
            query = """
                BEGIN;
    
                 
                INSERT INTO transaction_history ( amount_paid, trnsc_date, tax_id)
                 VALUES ( %s, CURRENT_DATE, %s);
    
                -- Update the payment_taxes table
                 UPDATE payment_taxes
                SET due = due - %s
                WHERE tax_id = %s;

                COMMIT;
                """

            logging.debug(f"Executing SQL Transaction:\n{query}")
            logging.debug(f"Values: ( amount={amount}, tax_id={tax_id})")

            cur.execute(query, ( amount, tax_id, amount,  tax_id))
            conn.commit()
            logging.debug("Transaction committed successfully.")

            cur.close()
            conn.close()
            logging.debug("Database connection closed.")

            messages.success(request, "Payment recorded successfully!")
            if type == "citizens":
                return redirect("citizenTaxes")  # Redirect citizens to their tax payments page
            elif type == "panchayat_employees":
                return redirect("panchayat_employees")  # Redirect to dashboard after payment

        except psycopg2.Error as e:
            conn.rollback()  # Rollback in case of error
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            return redirect("villagedashboard")

    return render(request, "payments.html")

def mycertificates(request):

    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    
    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")
        citizen_id = request.session.get("id")

        # Fetch the certificate id, certificate type, issued date, event type 
        query = """
        SELECT cert.certificate_id, cert.type, cert.issue_date, cert.event_date
        FROM certificates AS cert
        INNER JOIN citizen_certificate AS cc ON cert.certificate_id = cc.certificate_id 
        WHERE cc.citizen_id = %s;
        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query, (citizen_id,))
        data = cur.fetchall()
        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")
        # Convert data into a list of dictionaries
        column_names = ["certificate_id", "certificate_type", "issue_date", "event_date"]
        records = [dict(zip(column_names, row)) for row in data]
        
        logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records = []

    logging.debug("Rendering certificates.html with records.")
    return render(request, "mycertificates.html", {"records": records})

def previousTransactions(request):

    tax_id = request.GET.get("tax_id")
    logging.debug(f"TAX ID = {tax_id}")

    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    
    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")
        

        # Fetch the certificate id, certificate type, issued date, event type 
        query = """
        SELECT transaction_id,trnsc_date,amount_paid
        FROM transaction_history
        WHERE tax_id = %s;
        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query, (tax_id,))
        data = cur.fetchall()
        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")
        # Convert data into a list of dictionaries
        column_names = ["transaction_id","trnsc_date", "amount_paid" ]
        records = [dict(zip(column_names, row)) for row in data]
        
        logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records = []

    logging.debug("Rendering certificates.html with records.")
    return render(request, "previousTransactions.html", {"records": records})

def land_records(request):
    logging.debug("land_records view called.")  # Debugging Start

    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')

    try:
        # logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        # logging.debug("Database connection established.")
        citizen_id = request.session.get("id")

        # Fetch Panchayat Employees data
        query = """
        SELECT 
        lo1.land_id AS Land_id,
        la.area_acres AS Area,
        la.type_l AS Land_type,
        CASE 
            WHEN COUNT(lo2.citizen_id) > 0 THEN 
                COALESCE(STRING_AGG(DISTINCT c2.nm, ', ' ORDER BY c2.nm), '') 
            ELSE 
                'NULL' 
        END AS Co_owners
        FROM land_ownership lo1
        JOIN land_acres la ON lo1.land_id = la.id
        LEFT JOIN land_ownership lo2 ON lo1.land_id = lo2.land_id AND lo1.citizen_id != lo2.citizen_id
        LEFT JOIN Citizens c2 ON lo2.citizen_id = c2.id
        WHERE lo1.citizen_id = %s AND la.stat = 'active'
        GROUP BY lo1.land_id, la.area_acres, la.type_l, lo1.citizen_id;

        """
        # Execute the query with the citizen_id parameter
        cur.execute(query, (citizen_id,))
        data = cur.fetchall()
        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")

        # Convert data into a list of dictionaries
        column_names = ["Land_id", "Area", "Type_of_land", "co_owners"]
        records = [dict(zip(column_names, row)) for row in data]
        
        logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records = []

    logging.debug("Rendering land_records.html with records.")
    return render(request, "land_records.html", {"records": records})

def citizenschemes(request):
    logging.debug("citizenschemes view called.")

    # **Check if the user is logged in (flag must be 1)**
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to view tax details.")
        return redirect("login")  # Redirect to login page

    # **Check if citizen_id exists in session**
    citizen_id = request.session.get("id")
    if not citizen_id:
        messages.error(request, "Invalid session. Please log in again.")
        return redirect("login")

    try:
        logging.debug(f"Fetching Schemes for citizen_id: {citizen_id}")

        conn = get_db_connection()
        cur = conn.cursor()

        # **Corrected Query using parameterized SQL**
        query = """
        SELECT ws.nm,se.enrollment_date 
        FROM scheme_enrollment se
        JOIN welfare_scheme ws ON se.scheme_id=ws.scheme_id
        JOIN citizens c ON se.citizen_id = c.id
        WHERE c.id=%s;
        """
        cur.execute(query, (citizen_id,))
        data = cur.fetchall()

        cur.close()
        conn.close()

        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        # **Convert data into a list of dictionaries with S.No**
        column_names = ["nm","enrollment_date"]
        records = [{"s_no": idx + 1, **dict(zip(column_names, row))} for idx, row in enumerate(data)]


        logging.debug(f"Processed records: {records}")

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)
        messages.error(request, error_message)
        records = []

    return render(request, "citizenschemes.html", {"records": records})

def citizensProfile(request):

    citizen_id = request.session.get("id")
    logging.debug(f"Citizen ID = {citizen_id}")

    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    
    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")
        

        # Fetch the certificate id, certificate type, issued date, event type 
        query = """
        SELECT 
        c.nm AS citizen_name, c.username, c.id, c.gender, c.dob, c.education_qualification, f.nm AS father_name, 
        m.nm AS mother_name, s.nm AS spouse_name, c.category, c.occupation, c.income
        FROM citizens c
        LEFT JOIN citizens f ON c.father = f.id
        LEFT JOIN citizens m ON c.mother = m.id
        LEFT JOIN citizens s ON c.spouse = s.id
        WHERE c.id = %s;
        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query, (citizen_id,))
        data = cur.fetchall()
        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")
        # Convert data into a list of dictionaries
        column_names = ["citizen_name","username","citizen_id","gender","dob","education_qualifications","fathername",
                        "mothername", "spousename", "category", "occupation", "income" ]
        records = [dict(zip(column_names, row)) for row in data]
        
        logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records = []

    logging.debug("Rendering citizenProfile.html with records.")
    return render(request, "citizenProfile.html", {"records": records})


def editCitizenProfile(request):
    citizen_id = request.session.get("id")

    if not citizen_id:
        messages.error(request, "You are not logged in.")
        return redirect("login")
    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    if  request.session.get("user_type") != "citizens":
        messages.error(request, "you are not a citizen .Login as govt Monitor")
        return redirect('login')
    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")

        # Fetch existing citizen details
        query = """
        SELECT 
        c.nm AS citizen_name, c.username, c.id, c.gender, c.dob, 
        c.education_qualification, c.occupation
        FROM citizens c
        WHERE c.id = %s;
        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query, (citizen_id,))
        data = cur.fetchone()  
        logging.debug(f"Query executed successfully. Retrieved record: {data}")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")

        if not data:
            messages.error(request, "Citizen not found.")
            return redirect("citizensProfile")


        column_names = ["citizen_name", "username", "citizen_id", "gender", "dob", "education_qualification", "occupation"]
        citizen_data = dict(zip(column_names, data))

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        messages.error(request, f"Database error: {e}")
        return redirect("citizensProfile")

   
    if request.method == "GET":
        return render(request, "editCitizenProfile.html", {"citizen": citizen_data})

 
    elif request.method == "POST":
        education_qualification = request.POST.get("education_qualification")
        occupation = request.POST.get("occupation")
        new_password = request.POST.get("password")

        try:
            conn = get_db_connection()
            cur = conn.cursor()

            update_query = """
            UPDATE citizens
            SET education_qualification = %s, occupation = %s
            WHERE id = %s;
            """
            values = (education_qualification, occupation, citizen_id)

            logging.debug(f"Executing update query: {update_query} with values {values}")
            cur.execute(update_query, values)

        
            if new_password:
                hashed_password = make_password(new_password)
                cur.execute("UPDATE citizens SET passwd = %s WHERE id = %s;", (hashed_password, citizen_id))

            conn.commit()
            cur.close()
            conn.close()
            logging.debug("Citizen profile updated successfully.")

            messages.success(request, "Profile updated successfully!")
            return redirect("citizensProfile")  # Redirect back to profile page

        except psycopg2.Error as e:
            conn.rollback()  # Rollback on failure
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            return redirect("editCitizenProfile")

    return render(request, "editCitizenProfile.html", {"citizen": citizen_data})



def govt_monitors(request):
    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    if  request.session.get("user_type") != "govt_monitors":
        messages.error(request, "you are not a govt monitor .Login as govt Monitor")
        return redirect('login')
    try:
        # logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        # logging.debug("Database connection established.")

        # Fetch revenue report
        query = """
        WITH RECURSIVE years AS (
        SELECT MIN(EXTRACT(YEAR FROM enrollment_date))::INT AS year FROM scheme_enrollment
        UNION
        SELECT year + 1 FROM years WHERE year < EXTRACT(YEAR FROM CURRENT_DATE)
        ),

        wlf_amount AS (
            SELECT EXTRACT(YEAR FROM se.enrollment_date)::INT AS year,
                SUM(ws.scheme_amt) AS scheme_amount
            FROM scheme_enrollment AS se
            JOIN welfare_scheme AS ws ON se.scheme_id = ws.scheme_id
            GROUP BY year
        ),

        salaries AS (
            SELECT y.year, SUM(pe.salary)::INT AS salary
            FROM years y
            CROSS JOIN panchayat_employees pe
            GROUP BY y.year
        ),

        asset_exp AS (
            SELECT EXTRACT(YEAR FROM ae.spent_date)::INT AS year,
                SUM(ae.amount_spent) AS asset_exp
            FROM assets_expenditure AS ae
            GROUP BY year
        ),

        tax AS (
            SELECT EXTRACT(YEAR FROM th.trnsc_date)::INT AS year,
                SUM(th.amount_paid) AS tax
            FROM transaction_history AS th
            GROUP BY year
        ),

        scrap AS (
            SELECT EXTRACT(YEAR FROM a.demolition_date)::INT AS year,
                SUM(a.scrap_cost) AS scrap
            FROM assets AS a
            GROUP BY year
        )

        SELECT y.year AS Year,
            COALESCE(sa.salary, 0) AS Salaries,
            COALESCE(ae.asset_exp, 0) AS Asset_Exp,
            COALESCE(t.tax, 0) AS Tax,
            COALESCE(sc.scrap, 0) AS Scrap,
            COALESCE(wa.scheme_amount, 0) AS Scheme,
            (COALESCE(t.tax, 0) + COALESCE(sc.scrap, 0) - COALESCE(wa.scheme_amount, 0) - COALESCE(sa.salary, 0) - COALESCE(ae.asset_exp, 0)) AS Net_Amount
        FROM years y
        LEFT JOIN salaries sa ON y.year = sa.year
        LEFT JOIN asset_exp ae ON y.year = ae.year
        LEFT JOIN tax t ON y.year = t.year
        LEFT JOIN scrap sc ON y.year = sc.year
        LEFT JOIN wlf_amount wa ON y.year = wa.year
        ORDER BY Year;

        """
        # logging.debug(f"Executing query: {query}")
        cur.execute(query)
        rev_rep_data = cur.fetchall()
        # logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")
        
        query="""
        WITH years AS (
        SELECT DISTINCT EXTRACT(YEAR FROM se.enrollment_date)::INT AS year
        FROM scheme_enrollment se
        UNION
        SELECT DISTINCT EXTRACT(YEAR FROM CURRENT_DATE)::INT -- To include the current year
        ),

        schemes AS (
        SELECT nm, scheme_id
        FROM welfare_scheme
        ),

        all_combinations AS (
        SELECT y.year, s.nm, s.scheme_id
        FROM years y
        CROSS JOIN schemes s
        ),

        enrollments AS (
        SELECT EXTRACT(YEAR FROM se.enrollment_date)::INT AS year, 
        COUNT(se.citizen_id) AS No_Of_citizens, 
        se.scheme_id
        FROM scheme_enrollment se
        GROUP BY year, se.scheme_id
        )

        SELECT ac.year, ac.nm AS Scheme_Name, COALESCE(e.No_Of_citizens, 0) AS No_Of_citizens
        FROM all_combinations ac
        LEFT JOIN enrollments e ON ac.year = e.year AND ac.scheme_id = e.scheme_id
        ORDER BY ac.year, ac.nm;

        """
        cur.execute(query)
        welfare_data = cur.fetchall()
        query="""
        SELECT 
        EXTRACT(YEAR FROM v.date_administered)::INT AS year,
        v.vaccine_type,
        COUNT(DISTINCT v.citizen_id) AS No_Of_Citizens
        FROM 
        vaccinations v
        GROUP BY 
        year, v.vaccine_type
        ORDER BY 
        year, v.vaccine_type;

        """
        cur.execute(query)
        vaccine_data = cur.fetchall()
        query="""
        SELECT ar.yr AS year, 
       ar.crop_type, 
       SUM(la.area_acres) AS total_acres
       FROM agricultural_records ar
       JOIN land_acres la ON ar.land_id = la.ID
       GROUP BY ar.yr, ar.crop_type
       ORDER BY ar.yr;


        """
        cur.execute(query)
        agri_data = cur.fetchall()
        query = """
        SELECT 
        year_series AS year_end,
        COUNT(c.ID) AS total_population,

        COUNT(CASE WHEN c.gender = 'Male' THEN 1 END) AS male_count,
        COUNT(CASE WHEN c.gender = 'Female' THEN 1 END) AS female_count,

        COUNT(CASE WHEN year_series - EXTRACT(YEAR FROM c.DOB) < 18 THEN 1 END) AS child_count,
        COUNT(CASE WHEN year_series - EXTRACT(YEAR FROM c.DOB) > 60 THEN 1 END) AS senior_citizen_count,

        COUNT(CASE WHEN EXTRACT(YEAR FROM c.DOB) = year_series THEN 1 END) AS number_of_births,
        COUNT(CASE WHEN EXTRACT(YEAR FROM c.date_of_death) = year_series THEN 1 END) AS number_of_deaths,

        ROUND(
            (COUNT(CASE 
                WHEN c.education_qualification IS NOT NULL 
                AND c.education_qualification <> '' 
                AND LOWER(c.education_qualification) <> 'illiterate' 
            THEN 1 END) * 100.0) / COUNT(c.ID), 2
        ) AS literacy_rate,

        ROUND(
            (COUNT(CASE 
                WHEN c.occupation IS NOT NULL 
                AND c.occupation <> '' 
                AND c.occupation NOT IN ('Unemployed', 'Housewife') 
            THEN 1 END) * 100.0) / COUNT(c.ID), 2
        ) AS employment_rate

        FROM 
            generate_series(2000, CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER), 1) AS year_series

        LEFT JOIN 
            citizens c ON (EXTRACT(YEAR FROM c.DOB) <= year_series 
            AND (c.date_of_death IS NULL OR EXTRACT(YEAR FROM c.date_of_death) >= year_series))

        GROUP BY 
            year_series

        ORDER BY 
            year_series;
        """
        cur.execute(query)
        census_data = cur.fetchall()
        query="""
        SELECT 
        year_series AS year,

        COALESCE(SUM(CASE 
        WHEN la.type_l = 'Agriculture'
        AND (year_series BETWEEN EXTRACT(YEAR FROM lo.from_date) 
        AND COALESCE(EXTRACT(YEAR FROM lo.to_date), CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)))
        AND NOT (
            year_series = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)
            AND lo.to_date IS NOT NULL
            AND EXTRACT(YEAR FROM lo.to_date) = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)
        )
        THEN la.area_acres 
        ELSE 0 
        END), 0) AS total_agricultural_land,

        COALESCE(SUM(CASE 
        WHEN la.type_l = 'Non-Agriculture'
        AND (year_series BETWEEN EXTRACT(YEAR FROM lo.from_date) 
        AND COALESCE(EXTRACT(YEAR FROM lo.to_date), CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)))
        AND NOT (
            year_series = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)
            AND lo.to_date IS NOT NULL
            AND EXTRACT(YEAR FROM lo.to_date) = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)
        )
        THEN la.area_acres 
        ELSE 0 
        END), 0) AS total_non_agricultural_land,

        COALESCE(SUM(CASE 
        WHEN (year_series BETWEEN EXTRACT(YEAR FROM lo.from_date) 
        AND COALESCE(EXTRACT(YEAR FROM lo.to_date), CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)))
        AND NOT (
            year_series = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)
            AND lo.to_date IS NOT NULL
            AND EXTRACT(YEAR FROM lo.to_date) = CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER)
        )
        THEN la.area_acres 
        ELSE 0 
        END), 0) AS total_land

        FROM 
        generate_series(
        (SELECT MIN(EXTRACT(YEAR FROM from_date))::INTEGER FROM land_ownership), 
        CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER), 
        1
        ) AS year_series

        LEFT JOIN 
        land_ownership lo ON (
            year_series BETWEEN EXTRACT(YEAR FROM lo.from_date) 
            AND COALESCE(EXTRACT(YEAR FROM lo.to_date), CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS INTEGER))
        )

        LEFT JOIN 
            land_acres la ON lo.land_id = la.ID

        GROUP BY 
            year_series

        ORDER BY 
            year_series;

        """

        cur.execute(query)
        land_data = cur.fetchall()
        query="""
        SELECT 
        pt.yr AS year,
        COALESCE(SUM(pt.total_amount), 0) AS total_tax,
        COALESCE(SUM(pt.total_amount - pt.due), 0) AS total_paid,
        COALESCE(SUM(pt.due), 0) AS total_due

        FROM 
        payment_taxes pt

        GROUP BY 
        pt.yr

        ORDER BY 
        pt.yr;

        """
        cur.execute(query)
        tax_data = cur.fetchall()
        rr_column_names = ["year","salaries", "asset_exp", "tax","scrap","scheme","net_amount"]
        rr_records = [{"s_no": idx + 1, **dict(zip(rr_column_names, row))} for idx, row in enumerate(rev_rep_data)]
        welf_column_names = ["year","scheme_name", "no_of_citizens"]
        welf_records = [{"s_no": idx + 1, **dict(zip(welf_column_names, row))} for idx, row in enumerate(welfare_data)]
        vacc_column_names = ["year","vaccine_type", "no_of_citizens"]
        vacc_records = [{"s_no": idx + 1, **dict(zip(vacc_column_names, row))} for idx, row in enumerate(vaccine_data)]
        agri_column_names = ["year","crop_type", "total_acres"]
        agri_records = [{"s_no": idx + 1, **dict(zip(agri_column_names, row))} for idx, row in enumerate(agri_data)]
        census_column_names = ["year_end","total_population", "male_count", "female_count", "child_count", "senior_citizen_count", "number_of_births","number_of_deaths", "literacy_rate","employment_rate"]
        census_records = [{"s_no": idx + 1, **dict(zip(census_column_names, row))} for idx, row in enumerate(census_data)]
        land_column_names = ["year","total_agricultural_land", "total_non_agricultural_land", "total_land"]
        land_records = [{"s_no": idx + 1, **dict(zip(land_column_names, row))} for idx, row in enumerate(land_data)]
        tax_column_names = ["year","total_tax", "total_paid", "total_due"]
        tax_records = [{"s_no": idx + 1, **dict(zip(tax_column_names, row))} for idx, row in enumerate(tax_data)]
    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return render(request,"govt_monitors.html",{"records": rr_records,"welf_records":welf_records,"vacc_records":vacc_records,"agri_records":agri_records,"census_records":census_records,"land_records":land_records,"tax_records":tax_records})

def Admin(request):
    # logging.debug("admin is requested")
    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    if  request.session.get("user_type") != "Admin":
        messages.error(request, "you are not an Admin .Login as Admin")
        return redirect('login')
    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")

        # Fetch revenue report
        query = """
        SELECT id,nm,stat
        FROM govt_monitors

        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query)
        govt_monitors_data = cur.fetchall()
        logging.debug(f"Query executed successfully. Retrieved {len(govt_monitors_data)} records.")

        query = """
        SELECT p.id,nm,job_role,stat
        FROM panchayat_employees as p,citizens
        WHERE p.citizen_id = citizens.id

        """
        # logging.debug(f"Executing query: {query}")
        cur.execute(query)
        panchayat_employees_data = cur.fetchall()
        # logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")
        
        GM_column_names = ["id","name","status"]
        GM_records = [{"s_no": idx + 1, **dict(zip(GM_column_names, row))} for idx, row in enumerate(govt_monitors_data)]
        
        PE_column_names = ["id","name","job_role","status"]
        PE_records = [{"s_no": idx + 1, **dict(zip(PE_column_names, row))} for idx, row in enumerate(panchayat_employees_data)]
       
    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return render(request,"Admin.html",{"GM_records": GM_records,"PE_records":PE_records})

   

def addGovtMonitor_admin(request):
    logging.debug("addgovtMonitor_admin view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        name = request.POST.get("nm")
        
        
        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            
            query = """
                INSERT INTO govt_monitors (nm,stat,username,passwd )
                VALUES (%s,%s,NULL,NULL);
            """
            
            values = (name,"active")
            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "Citizen added successfully.")
            return redirect("Admin")  # Redirect to admin dashboard after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            
    return render(request, "addGovtMonitor_admin.html")

def addemployee_admin(request):
    logging.debug("addcitizen_admin view called.")
    
    # Check if the user is logged in (flag must be 1)
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    if request.method == "POST":
        citizen_id = request.POST.get("citizen_id")
        job_role = request.POST.get("job_role")
        salary = request.POST.get("salary")

        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")
            
            # Convert empty strings to NULL for optional fields
            salary = float(salary) if salary else None

            query = """
                INSERT INTO panchayat_employees (citizen_id, job_role, salary, username, passwd, stat)
                VALUES (%s, %s, %s, NULL, NULL,'active');
            """
            
            values = (citizen_id, job_role, salary)
            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            cur.close()
            conn.close()
            logging.debug("Database connection closed.")
            messages.success(request, "Citizen added successfully.")
            return redirect("Admin")  # Redirect to admin dashboard after success
            
        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            
        except ValueError as e:
            logging.error(f"Value error: {e}")
            messages.error(request, "Invalid data format.")
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            
    return render(request, "addemployee_admin.html")

def inactiveGM(request):
    # logging.debug("admin is requested")
    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    if  request.session.get("user_type") != "Admin":
        messages.error(request, "you are not an Admin .Login as Admin")
        return redirect('login')
    
    id = request.GET.get("GM_id")
    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")

        # Fetch revenue report
        query = """
        UPDATE govt_monitors
        SET stat = 'inactive'
        WHERE id = %s

        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query,(id,))
        conn.commit()

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")

        messages.success(request, "Government Monitor set to inactive successfully!")
        

        
    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return redirect("Admin")

def inactivePE(request):
    if  request.session.get("flag") != 1:
        messages.error(request, "you are not logged in")
        return redirect('login')
    if  request.session.get("user_type") != "Admin":
        messages.error(request, "you are not an Admin .Login as Admin")
        return redirect('login')
    
    id = request.GET.get("PE_id")
    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")

        # Fetch revenue report
        query = """
        UPDATE panchayat_employees
        SET stat = 'inactive'
        WHERE id = %s

        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query,(id,))
        conn.commit()

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")

        messages.success(request, "Panchayat employee set to inactive successfully!")
        

        
    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return redirect("Admin")

def updateLandRecord(request):
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a citizen.")
        return redirect("login")
    
    land_id = request.GET.get("land_id")  # Get land_id from URL query params

    if request.method == "POST":
        year = request.POST.get("year")
        crop_type = request.POST.get("crop_type")

        logging.debug(f"Updating land record for land_id={land_id}, year={year}, crop_type={crop_type}")

        if not year or not crop_type:
            messages.error(request, "Please fill in all fields.")
            return redirect(f'updateLandRecord/?land_id={land_id}')

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")

            # **SQL Update Statement for agricultural_records**
            query = """
            INSERT INTO agricultural_records(yr, crop_type, land_id)
            VALUES
            (%s, %s, %s);
            """

            logging.debug(f"Executing SQL Query: {query}")
            cur.execute(query, (year, crop_type, land_id))
            conn.commit()
            logging.debug("Record updated successfully.")

            cur.close()
            conn.close()
            logging.debug("Database connection closed.")

            messages.success(request, "Land record updated successfully!")
            return redirect("land_records")  # Redirect to land records page

        except psycopg2.Error as e:
            conn.rollback()  # Rollback in case of error
            logging.error(f"Database error: {e}")
            messages.error(request, f"Database error: {e}")
            return redirect(f'updateLandRecord/?land_id={land_id}')

    return render(request, "updateLandRecord.html", {"land_id": land_id})

def crop_history(request):
    land_id = request.GET.get("land_id")  # Get the land_id from the query parameters
    logging.debug(f"LAND ID = {land_id}")

    # Check if the user is logged in
    if request.session.get("flag") != 1:
        messages.error(request, "You are not logged in.")
        return redirect('login')

    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")
        
        # Fetch year and crop type based on land_id
        query = """
        SELECT yr, crop_type
        FROM agricultural_records
        WHERE land_id = %s
        ORDER BY yr;
        """
        logging.debug(f"Executing query: {query}")
        cur.execute(query, (land_id,))
        data = cur.fetchall()
        logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")

        # Convert data into a list of dictionaries
        column_names = ["yr", "crop_type"]
        records = [dict(zip(column_names, row)) for row in data]
        
        logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records = []

    logging.debug("Rendering crop_history.html with records.")
    return render(request, "crop_history.html", {"records": records})


def updateLand(request):
    records = {}
    if request.method == "POST":
        # Get form data from POST request
        land_id=request.POST.get("land_id")
        owner_type=request.POST.get("ownerType")
        owner_id=request.POST.get("owner_id")
        logging.debug(f"Received form data: {request.POST}")

        # Validate input (ensure required fields are not empty)
        if not all([land_id,owner_type]):
            messages.error(request, "All fields are required.")
            return redirect("panchayat_employees")

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established for UPDATE.")
            if owner_type=="mutual_owner":
                # Update query
                update_query = """
                    INSERT INTO land_ownership values
                    (%s,%s,CURRENT_DATE,NULL);
                """
                cur.execute(update_query, (int(land_id),int(owner_id)))
                conn.commit()

                logging.debug(f"Database updated successfully for ID: {land_id}.")
                messages.success(request, "Citizen profile updated successfully.")

                # Close DB connection
                cur.close()
                conn.close()
                logging.debug("Database connection closed after update.")

                return redirect("panchayat_employees")  # Redirect after successful update
            # Update query
            update_query = """
                BEGIN;
                UPDATE land_ownership
                SET to_date=CURRENT_DATE
                WHERE land_id=%s;
                INSERT INTO land_ownership
                VALUES (%s,%s,CURRENT_DATE,NULL);
                
                COMMIT;
            """
            cur.execute(update_query, (land_id,land_id,owner_id,))
            conn.commit()

            logging.debug(f"Database updated successfully for ID: {land_id}.")
            messages.success(request, "Land profile updated successfully.")

            # Close DB connection
            cur.close()
            conn.close()
            logging.debug("Database connection closed after update.")

            return redirect("panchayat_employees")  # Redirect after successful update

        except psycopg2.Error as e:
            logging.error(f"Database update error: {e}")
            messages.error(request, f"Database error: {e}")

    elif request.method == "GET":
        # Get ID from URL or fall back to session ID
        id=request.GET.get("land_id")    
        logging.debug(f"ID retrieved: {id} ")

        if not id:
            messages.error(request, "User ID not found in session or URL.")
            logging.error("User ID not found in session or URL.")
            return render(request, "updateCitizen.html", {"record": records})

        try:
            id = int(id)  # Ensure ID is an integer
        except ValueError:
            messages.error(request, "Invalid ID format.")
            logging.error("Invalid ID format received.")
            return render(request, "updateCitizen.html", {"record": records})

            # If GET request, fetch the existing citizen data
        try:
            logging.debug("Connecting to the database for GET request...")
            conn = get_db_connection()
            cur = conn.cursor()

            query = """
                SELECT land_id,citizen_id,from_date,to_date
                FROM land_ownership
                WHERE land_id = %s;
            """
            logging.debug(f"Executing query: {query} with id: {id}")
            cur.execute(query, (id,))  
            c_data = cur.fetchone()  # Fetch a single row
            logging.debug(f"Query executed successfully. Retrieved data: {c_data}")

            if c_data:
                c_columns = ["id", "owner_id", "from_date", "to_date"]
                records = dict(zip(c_columns, c_data))
                logging.debug(f"Final records sent to template: {records}")

            cur.close()
            conn.close()
            logging.debug("Database connection closed.")

        except psycopg2.Error as e:
            logging.error(f"Database fetch error: {e}")
            messages.error(request, f"Database error: {e}")
        
        return render(request, "updateLand.html", {"land": records})
    

def previousOwners(request):
    land_id = request.GET.get("land_id")  
    logging.debug(f"LAND ID = {land_id}")

    if request.session.get("flag") != 1:
        messages.error(request, "You are not logged in")
        return redirect("login")

    try:
        logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        logging.debug("Database connection established.")

        records = []
        current_land_id = land_id  

        while current_land_id:
            query = """
            SELECT 
             c.nm AS citizen_name,
             lo.from_date, 
             lo.to_date, 
             la.old_id
             FROM land_ownership AS lo
             LEFT JOIN land_acres AS la ON lo.land_id = la.id
             LEFT JOIN citizens AS c ON lo.citizen_id = c.id  
             WHERE lo.land_id = %s;

            """
            logging.debug(f"Executing query for land_id: {current_land_id}")
            cur.execute(query, (current_land_id,))
            data = cur.fetchone() 

            if not data :
                break  

            
            records.append({
                "citizen_name": data[0],
                "from_date": data[1],
                "to_date": data[2]
            })

            current_land_id = data[3] 
            logging.debug(f"Moving to previous owner: {current_land_id}")

        cur.close()
        conn.close()
        logging.debug("Database connection closed.")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        messages.error(request, f"Database error: {e}")
        records = []

    logging.debug(f"Processed records: {records}")
    return render(request, "previousOwners.html", {"records": records})

def addcomplaints(request):
    logging.debug("addcomplaints view called.")
    
    # Check if the user is logged in
    if request.session.get("flag") != 1:
        messages.error(request, "You must be logged in to add a complaint.")
        return redirect("login")
    
    if request.method == "POST":
        citizen_id = request.session.get("id")  # Fetch Citizen ID from session
        enrollment_date = date.today()
        description = request.POST.get("description")

        if not description:
            messages.error(request, "Description cannot be empty.")
            return redirect("addcomplaints")

        try:
            logging.debug("Attempting to connect to the database...")
            conn = get_db_connection()
            cur = conn.cursor()
            logging.debug("Database connection established.")

            # Insert Query
            query = """
                INSERT INTO complaints (citizen_id, enrolled_date, description)
                VALUES (%s, %s, %s);
            """
            values = (citizen_id, enrollment_date, description)

            logging.debug(f"Executing SQL Query: {query} with values {values}")
            
            cur.execute(query, values)
            conn.commit()
            logging.debug("Transaction committed successfully.")
            
            messages.success(request, "Complaint added successfully.")
            return redirect("citizens")
        
        except psycopg2.IntegrityError as e:
            conn.rollback()
            logging.error(f"Foreign Key Violation: {e}")
            messages.error(request, "Invalid Citizen ID. Please try again.")

        except psycopg2.Error as e:
            conn.rollback()
            logging.error(f"Database error: {e}")
            messages.error(request, "Database error occurred.")

        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    return render(request, "addcomplaints.html")


def logout(request):
    request.session["flag"] = 0
    return redirect("home")

