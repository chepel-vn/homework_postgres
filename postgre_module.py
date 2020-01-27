import psycopg2
import postgres_consts


def request_decorator(function_to_decorate):
    """

    (function link) -> function link

    Function decorator for use connection and cursor for access to database

    """

    def wrapper(*args, **kwargs):
        conn = None
        curs = None
        err = (None, 0)
        try:
            conn = psycopg2.connect(postgres_consts.CONNECT_STRING)
            conn.autocommit = False
            curs = conn.cursor()

            # Function for select, insert, update request
            function_to_decorate(curs, *args, **kwargs)
            try:
                result_records = curs.fetchall()
                err = (result_records, 0)
            except psycopg2.ProgrammingError:
                err = (None, 0)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error in transaction Reverting all other operations of a transaction ", error)
            if conn is not None:
                conn.rollback()
            err = (None, -1)
        finally:
            if conn is not None:
                conn.commit()
                curs.close()
                conn.close()
            return err
    return wrapper


@request_decorator
def drop_db(curs):
    """

    (cursor link) -> None

    Function drops all tables

    """

    curs.execute("""DROP TABLE if exists student_course;""")
    curs.execute("""DROP TABLE if exists course;""")
    curs.execute("""DROP TABLE if exists student;""")


@request_decorator
def create_db(curs):
    """

    (cursor link) -> None

    Function creates all tables

    """

    curs.execute("""CREATE TABLE if not exists student (
                id serial PRIMARY KEY ,
                name varchar(100) NOT NULL,
                gpa numeric(10,2),
                birth timestamp with time zone);
                """)

    curs.execute("""CREATE TABLE if not exists course (
                id serial PRIMARY KEY ,
                name varchar(100) NOT NULL);
                """)

    curs.execute("""CREATE TABLE if not exists student_course (
                id serial PRIMARY KEY,
                student_id integer references student(id),
                course_id integer references course(id));
                """)


@request_decorator
def add_student(curs, fio, birthday):
    """

    (cursor link, string, string) -> integer

    Function add one student

    """

    curs.execute("insert into student (name, birth) values (%s, %s) returning id", (f"{fio}", f"{birthday}"))
    # student_id = curs.fetchone()[0]
    # return (student_id, 0)


@request_decorator
def add_student_to_course(curs, student_id, course_id):
    """

    (cursor link, integer, integer) -> None

    Function adds students to course

    """

    curs.execute("select id from student_course where student_id = %s and course_id = %s",
                 (f"{student_id}", f"{course_id}"))
    if len(curs.fetchall()) <= 0:
        curs.execute("insert into student_course (student_id, course_id) values (%s, %s)", (student_id, course_id))


@request_decorator
def add_students(curs, course_id, students_list):
    """

    (cursor link, integer, integer) -> None

    Function creates students and adds these students to course

    """

    for fio, birthday in students_list.items():
        curs.execute("insert into student (name, birth) values (%s, %s) returning id", (f"{fio}", f"{birthday}"))
        student_id = curs.fetchone()[0]
        if student_id is not None:
            curs.execute("insert into student_course (student_id, course_id) values (%s, %s)",
                         (f"{student_id}", f"{course_id}"))


@request_decorator
def get_student(curs, student_id):
    """

    (cursor link, integer, integer) -> None

    Function creates students and adds these students to course

    """

    curs.execute("select * from student where id = %s", f"{student_id}")


@request_decorator
def get_students(curs, course_id):
    """

    (cursor link, integer) -> None

    Function creates students and adds these students to course

    """

    curs.execute("select s.id, s.name, s.gpa, s.birth, c.id, c.name from student_course sc "
                 "join student s on s.id = sc.student_id "
                 "join course c on c.id = sc.course_id where c.id = %s", f"{course_id}")


@request_decorator
def add_course(curs, course_name):
    """

    (cursor link, integer, integer) -> None

    Function adds course

    """

    curs.execute("insert into course (name) values (%s) returning id", (f"{course_name}",))
    course_id = curs.fetchone()[0]
    return course_id


def add_courses(courses):
    for course_name in courses:
        add_course(course_name)


@request_decorator
def print_table(curs, table_name):
    curs.execute(f"select * from {table_name}")
    for rows in curs.fetchall():
        print(rows)


def print_student_list(caption, students_list):
    print(caption)
    for item in students_list:
        if item[3] is not None:
            birth = item[3].strftime("%m/%d/%Y")
        else:
            birth = ""
        print(f"{item[0]}. {item[1]}, {birth}")


def main():
    msg, err = drop_db()
    if err == -1:
        return -1

    msg, err = create_db()
    if err == -1:
        return -1

    add_courses(postgres_consts.courses)
    # print("add students of course 1")
    add_students(1, postgres_consts.students_course1)
    # print("add students of course 2")
    add_students(2, postgres_consts.students_course2)

    print("Добавим студента Соколова Василия...")
    student_id, err = add_student("Соколов Василий", "03.04.1979")
    if err == 0:
        print(f"Идентификатор Соколов Василий = {student_id[0][0]}")

    course_id = 1
    students, err = get_students(course_id)
    if err == 0:
        print_student_list(f"Студенты курса {course_id}:", students)

    course_id = 2
    students, err = get_students(course_id)
    if err == 0:
        print_student_list(f"Студенты курса {course_id}:", students)

    student, err = get_student(2)
    if err == 0:
        print_student_list("Поиск студента по id=2:", student)


if __name__ == '__main__':
    main()
