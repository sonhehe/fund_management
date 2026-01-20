from sqlalchemy import text
from scripts.db_engine import get_engine
import bcrypt


def hash_password(password: str) -> str:
    return bcrypt.hashpw(
        password.encode("utf-8"),
        bcrypt.gensalt()
    ).decode("utf-8")




def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(
        password.encode("utf-8"),
        hashed.encode("utf-8")
    )
# ======================
# LOGIN
# ======================


# ======================
# ADMIN LOGIN
# ======================
def authenticate_admin(username: str, password: str):
    engine = get_engine()


    with engine.connect() as conn:
        user = conn.execute(
            text("""
                SELECT
                    username,
                    role,
                    pw_hash
                FROM users
                WHERE username = :u
                  AND role = 'admin'
            """),
            {"u": username}
        ).mappings().fetchone()


    if user is None:
        return None


    if not verify_password(password, user["pw_hash"]):
        return None






    return {
        "username": user["username"],
        "role": user["role"],
        "customer_id": None,
        "customer_name": None,
    }




# ======================
# INVESTOR / ORGANISE LOGIN
# ======================
def authenticate_user(username: str, password: str):
    engine = get_engine()


    with engine.connect() as conn:
        user = conn.execute(
            text("""
                SELECT
                    u.username,
                    u.role,
                    u.pw_hash,
                    i.customer_id,
                    i.customer_name
                FROM users u
                JOIN investors i
                    ON u.customer_id = i.customer_id
                WHERE u.username = :u
                  AND u.role IN ('investor', 'organise')
            """),
            {"u": username}
        ).mappings().fetchone()


    if user is None:
        return None


    if not verify_password(password, user["pw_hash"]):
        return None






    return {
        "username": user["username"],
        "role": user["role"],
        "customer_id": user["customer_id"],
        "customer_name": user["customer_name"],
    }


# ======================
# REGISTER
# ======================
def register_user(data: dict):
    engine = get_engine()
    data["pw_hash"] = hash_password(data["pw_hash"])


    with engine.begin() as conn:
        # 1. Check trùng username / email
        existed = conn.execute(
            text("""
                SELECT 1 FROM users
                WHERE username = :u OR email = :e
            """),
            {
                "u": data["username"],
                "e": data["email"]
            }
        ).fetchone()


        if existed:
            return {"error": "Username hoặc email đã tồn tại"}


        # 2. Sinh customer_id
        prefix = "CN" if data["role"] == "investor" else "TC"


        last_id = conn.execute(
            text("""
                SELECT MAX(customer_id) FROM investors
                WHERE customer_id LIKE :p
            """),
            {"p": f"{prefix}%"}
        ).scalar()


        if last_id:
            num = int(last_id.replace(prefix, ""))
            customer_id = f"{prefix}{num + 1:02d}"
        else:
            customer_id = f"{prefix}01"


        data["customer_id"] = customer_id


        # 3. Insert users
        conn.execute(
            text("""
                INSERT INTO users (
                    username, customer_id, display_name, email, phone,
                    address, bank_account, pw_hash, role, created_at
                )
                VALUES (
                    :username, :customer_id, :display_name, :email, :phone,
                    :address, :bank_account, :pw_hash, :role, now()
                )
            """),
            data
        )


        # 4. Insert
        conn.execute(
            text("""
                INSERT INTO investors (
                    customer_id, customer_name, status,
                    open_account_date, identity_number,
                    dob, phone, email, address,
                    capital, nos, bank_account
                )
                VALUES (
                    :customer_id, :customer_name, 'Đang đầu tư',
                    CURRENT_DATE, :identity_number,
                    :dob, :phone, :email, :address,
                    0, 0, :bank_account
                )
            """),
            {
                "customer_id": customer_id,
                "customer_name": data["display_name"],
                "identity_number": data["cccd_mst"],
                "dob": data["dob"],
                "phone": data["phone"],
                "email": data["email"],
                "address": data["address"],
                "bank_account": data["bank_account"]
            }
        )


    return {"success": True, "customer_id": customer_id}




# ======================
# FORGOT PASSWORD (demo)
# ======================
def reset_password(username: str, new_password: str):
    engine = get_engine()


    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE users
                SET pw_hash = :pw
                WHERE username = :u
            """),
            {"u": username, "pw": hash_password(new_password)}


        )


    if result.rowcount == 0:
        return False


    return {"ok": True}





