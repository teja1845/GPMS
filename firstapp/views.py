from django.http import HttpResponse,HttpRequest
from django.template import loader
from django.shortcuts import render,redirect
import psycopg2
from django.contrib import messages
from datetime import datetime
from django.contrib.auth.hashers import make_password,check_password
import logging
from datetime import date

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
        host="10.5.18.69",
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
    # logging.debug("village_dashboard view called.")  # Debugging Start

    try:
        # logging.debug("Connecting to the database...")
        conn = get_db_connection()
        cur = conn.cursor()
        # logging.debug("Database connection established.")

        # Fetch Panchayat Employees data
        query = """
        SELECT c.nm, STRING_AGG(CAST(cm.mobile_no AS TEXT), ', ') AS mobile_numbers, pe.job_role 
        FROM panchayat_employees AS pe 
        INNER JOIN citizens AS c ON pe.citizen_id = c.id 
        INNER JOIN citizen_mobile AS cm ON c.id = cm.citizen_id 
        GROUP BY pe.id, c.nm, pe.job_role;
        """
        # logging.debug(f"Executing query: {query}")
        cur.execute(query)
        data = cur.fetchall()
        # logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")

        cur.close()
        conn.close()
        # logging.debug("Database connection closed.")

        # Convert data into a list of dictionaries
        column_names = ["name", "mobile_number", "designation"]
        records = [dict(zip(column_names, row)) for row in data]
        
        # logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records = []

    # logging.debug("Rendering villagedashboard.html with records.")
    return render(request, "villagedashboard.html", {"records": records})

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

        # Fetch land-ownership data
        query = """
        SELECT 
            la.ID as land_id,
            la.type_l as land_type,
            STRING_AGG(c.nm,', ') as list_of_owners
        FROM land_acres la
        LEFT JOIN land_ownership lo on la.ID=lo.land_id
        LEFT JOIN citizens c on lo.citizen_id=c.ID
        GROUP BY la.ID,la.type_l;
        """
        # logging.debug(f"Executing query: {query}")
        cur.execute(query)
        l_data = cur.fetchall()
        # logging.debug(f"Query executed successfully. Retrieved {len(data)} records.")
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
            COALESCE(SUM(ae.amount_spent), 0) AS total_expenditure
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

        #query to assets 
        query = """
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
        cur.execute(query)
        house_data = cur.fetchall()
        
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

        ast_column_names=["sno","ast_id","ast_name","loc","exp"]
        ast_records = [{"sno": idx + 1, **dict(zip(ast_column_names[1:], row))} for idx, row in enumerate(ast_data)]
        
        house_column_names=["sno","house_id","addr","income","members"]
        house_records = [{"sno": idx + 1, **dict(zip(house_column_names[1:], row))} for idx, row in enumerate(house_data)]
        # logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return render(request,"panchayat_employees.html",{"citizens_record":citizens_records,"land_records":land_records,"cer_records":cer_records,"tax_records":txn_records,"wel_records":wel_records,"sch_records":sch_records,"assets_records":ast_records,"house_records":house_records})

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
        citizen_id = request.session.get("id")  # Get citizen_id from session

        logging.debug(f"Received Amount: {amount}, Tax ID: {tax_id}, Citizen ID: {citizen_id}")

        if not citizen_id:
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

            INSERT INTO transaction_history (citizen_id, amount_paid, trnsc_date, tax_id)
            VALUES (%s, %s, CURRENT_DATE, %s);

            UPDATE payment_taxes
            SET due = due - %s
            WHERE citizen_id = %s AND tax_id = %s;

            COMMIT;
            """

            logging.debug(f"Executing SQL Transaction:\n{query}")
            logging.debug(f"Values: (citizen_id={citizen_id}, amount={amount}, tax_id={tax_id})")

            cur.execute(query, (citizen_id, amount, tax_id, amount, citizen_id, tax_id))
            conn.commit()
            logging.debug("Transaction committed successfully.")

            cur.close()
            conn.close()
            logging.debug("Database connection closed.")

            messages.success(request, "Payment recorded successfully!")
            return redirect("citizenTaxes")  # Redirect to dashboard after payment

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
        WHERE lo1.citizen_id = %s AND la.stat = 'Active'
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
        SELECT MIN(EXTRACT(YEAR FROM enrollment_date)) AS year FROM scheme_enrollment
        UNION
        SELECT year + 1 FROM years WHERE year < EXTRACT(YEAR FROM CURRENT_DATE)
        ),

        wlf_amount AS (
            SELECT EXTRACT(YEAR FROM se.enrollment_date) AS year,
                SUM(ws.scheme_amt) AS scheme_amount
            FROM scheme_enrollment AS se
            JOIN welfare_scheme AS ws ON se.scheme_id = ws.scheme_id
            GROUP BY year
        ),

        salaries AS (
            SELECT y.year, SUM(pe.salary) AS salary
            FROM years y
            CROSS JOIN panchayat_employees pe
            GROUP BY y.year
        ),

        asset_exp AS (
            SELECT EXTRACT(YEAR FROM ae.spent_date) AS year,
                SUM(ae.amount_spent) AS asset_exp
            FROM assets_expenditure AS ae
            GROUP BY year
        ),

        tax AS (
            SELECT EXTRACT(YEAR FROM th.trnsc_date) AS year,
                SUM(th.amount_paid) AS tax
            FROM transaction_history AS th
            GROUP BY year
        ),

        scrap AS (
            SELECT EXTRACT(YEAR FROM a.demolition_date) AS year,
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
        SELECT DISTINCT EXTRACT(YEAR FROM se.enrollment_date) AS year
        FROM scheme_enrollment se
        UNION
        SELECT DISTINCT EXTRACT(YEAR FROM CURRENT_DATE) -- To include the current year
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
        SELECT EXTRACT(YEAR FROM se.enrollment_date) AS year, 
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

        rr_column_names = ["year","salaries", "asset_exp", "tax","scrap","scheme","net_amount"]
        rr_records = [{"s_no": idx + 1, **dict(zip(rr_column_names, row))} for idx, row in enumerate(rev_rep_data)]
        welf_column_names = ["year","scheme_name", "no_of_citizens"]
        welf_records = [{"s_no": idx + 1, **dict(zip(welf_column_names, row))} for idx, row in enumerate(welfare_data)]
    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return render(request,"govt_monitors.html",{"records": rr_records,"welf_records":welf_records})


def logout(request):
    request.session["flag"] = 0
    return redirect("home")

