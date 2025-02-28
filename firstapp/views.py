from django.http import HttpResponse,HttpRequest
from django.template import loader
from django.shortcuts import render,redirect
import psycopg2
from django.contrib import messages
from django.contrib.auth.hashers import make_password,check_password
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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
        FROM citizens;
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

        # logging.debug(f"Processed records: {records}")  # Debugging Output

    except psycopg2.Error as e:
        error_message = f"Database error: {e}"
        logging.error(error_message)  # Log the error
        messages.error(request, error_message)
        records=[]

    return render(request,"panchayat_employees.html",{"citizens_record":citizens_records,"land_records":land_records,"cer_records":cer_records,"tax_records":txn_records})



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
        c.nm AS citizen_name, c.username, c.id, c.gender, c.dob, c.education_qualifications, f.nm AS father_name, 
        m.nm AS mother_name, s.nm AS spouse_name, c.category, c.occupation, c.income
        FROM citizens c
        LEFT JOIN citizens f ON c.father_id = f.id
        LEFT JOIN citizens m ON c.mother_id = m.id
        LEFT JOIN citizens s ON c.spouse_id = s.id
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


