"""
Main bot file. This file contains the main bot loop and the main functions to process the jobs.
"""
import asyncio
import glob
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import traceback
from multiprocessing.connection import Listener
from typing import Any, Optional

from aiohttp.client_exceptions import ClientConnectorError
from nextcord.ext.ipc.client import Client
from openai import OpenAI

from data_structure.Jobs import Job
from data_structure.PriorityQueue import StepQueue
from plib import terminal
from plib.db_handler import Database as Db
from plib.terminal import error

STEPS_CHART = {
    0: "Generate project files/structure",
    1: "Compilation test",
    2: "Running test cases",
    3: "Abstraction test",
    4: "ChatGPT consolidation",
    5: "Send to user"
}

async def main():
    """
    Main function to start the bot.
    """
    try:
        with open("credentials.txt", "r", encoding="utf-8") as f:
            
            CREDENTIALS = f.read().splitlines()
            os.environ["IPC_SECRET_KEY"] = CREDENTIALS[0]
            os.environ["OPENAI_API_KEY"] = CREDENTIALS[1]
    except FileNotFoundError:
        print("It seems that you don't have the required files. Please, read README.md#installation.")
        sys.exit(1)

    sq = StepQueue(len(STEPS_CHART.keys()))

    threads = [threading.Thread(target=asyncio.run, args=(worker(sq),)) for _ in range(3)]

    threads.append(threading.Thread(target=asyncio.run, args=(updater(sq, CREDENTIALS[0]),)))

    threads.append(threading.Thread(target=asyncio.run, args=(worker_done_job(sq),)))

    print(f"Created {len(threads)} threads.")

    for thread in threads:
        thread.start()
    
    print("Starting main loop...")
    ipc_client = Client(secret_key=CREDENTIALS[0])
    failed_tries = 0
    while failed_tries < 10:

        if sq.pause:
            await asyncio.sleep(5)

        data = sq.get_done_job(step=5)


        if data is None:
            continue

        step, job = data
        if job is None:
            continue
        
        if failed_tries > 0:
            ipc_client = Client(secret_key=CREDENTIALS[0])
        try:
            print("Checking IPC server...")
            ipc_status = await ipc_client.request("status")
            print(f"IPC server status: {ipc_status}")
            if ipc_status == 200:
                failed_tries = 0
                try:
                    payload = {
                        "id_user": job.id_user,
                        "id_exec": job.id_exec,
                        "project_type": job.project_type,
                        "category": job.category,
                        "broken": job.broken,
                        "text_content": job.text_content,
                        "gpt_content": job.gpt_content,
                        "abstraction_score": job.abstraction_score,
                        "banned_found": job.banned_found
                    }
                except AttributeError:
                    sq.clear()
                    sys.exit(1)
                await ipc_client.request("terminateJob", data=payload)
                sq.snapshot()
                shutil.rmtree(job.path, ignore_errors=True)
                continue
            failed_tries += 1
            print("IPC server not ready")
            await asyncio.sleep(5)
        except ClientConnectorError:
            failed_tries += 1
            print("IPC server not responding, trying again in 10 seconds...")
            await asyncio.sleep(10)
        except ConnectionResetError:
            failed_tries += 1
            print("IPC server connection reseted, trying again in 10 seconds...")
            await asyncio.sleep(10)
        sq.add_job(step, job)

async def worker(sq: StepQueue):
    """
    Main worker function. This function is the main loop for the worker threads.
    A worker thread is a thread that processes the jobs in the queue.

    Recommendation: Worker threads should reach all steps in the queue, but the last two, also known as "done" steps.

    Jobs being None: This happens when the queue receives a new job, and therefore unlocks the worker threads.
    Many workers will try to pull the job, but only one will get it.
    (If there is only one job and if there is many workers)
    
    Parameters
    ----------
    sq : `StepQueue`
        The step queue to process the jobs.
    """
    cooldown = 0
    while True:
        if sq.pause:
            await asyncio.sleep(3)
            if cooldown < 3:
                cooldown += 1
                continue
            cooldown = 0
    
        data = sq.get_next_job()

        if data is not None:
            step, job = data

            if job is None:
                continue
            
            process_job(job, step, sq)

        await asyncio.sleep(0.2)

async def worker_done_job(sq: StepQueue):
    """
    Special worker function. This function is the main loop for the done job worker threads.

    Recommendation: Done job worker threads should only reach the last two steps in the queue, also known as "done" steps.

    Jobs being None: This happens when the queue receives a new job, and therefore unlocks the worker threads.
    Many workers will try to pull the job, but only one will get it.
    (If there is only one job and if there is many workers)
    
    Parameters
    ----------
    sq : `StepQueue`
        The step queue to process the jobs.
    """
    cooldown = 0
    while True:
        if sq.pause_done:
            await asyncio.sleep(3)
            if cooldown < 3:
                cooldown += 1
                continue
            cooldown = 0

        data = sq.get_done_job(step=4)

        if data is not None:

            step, job = data

            if job is None:
                continue

            await process_done_job(job, step, sq)
        
        await asyncio.sleep(0.2)

def process_job(job: Job, step: int, sq: StepQueue):
    """
    Process a job in the queue, based on the step it is in.

    Normally, after processing the job, the function should add the job to the step queue again.
    The step at which the job is added depends on the result of the processing, usually relaying on job.broken.

    Step 0: Generate project files/structure
    Step 1: Compilation test
    Step 2: Running test cases
    Step 3: Abstraction test
    
    Parameters
    ----------
    job : `Optional[Job]`
        The job to process.
    step : `int`
        The step the job is in.
    sq : `StepQueue`
        The step queue to process the jobs.
    """
    print(f"Starting job with id= {job.id_exec} for user {job.id_user} at {job.path} on step {step}, which is '{STEPS_CHART[step]}'")
    
    if step == 0:
        status = generate_project_files(job)
        if not status:
            step = 5
            job.broken = True
            job.text_content = "No se han podido generar los archivos del proyecto. Si el error persiste, contacta a un administrador."
        else:
            step += 1
    elif step == 1:
        status, result = compilation_test(job)
        if not status:
            step = 4
            job.broken = True
            job.text_content = result[-1000:]
        else:
            step += 1
    elif step == 2:
        status = running_test(job)
        if status == 0:
            step += 1
        elif status == 1:
            step = 4
        else:
            step = 5

    elif step == 3:
        score, banned_found = abstraction_test(job)
        job.abstraction_score = score
        job.banned_found = banned_found
        step += 2
    sq.add_job(step, job)
    sq.snapshot() 

async def process_done_job(job: Job, step: int, sq: StepQueue):
    """
    Process a done job in the queue, based on the step it is in.

    Normally, after processing the job, the function should add the job to the step queue again.
    The step at which the job is added depends on the result of the processing, usually relaying on job.broken.

    Step 4: ChatGPT consolidation
    [Deprecated] Step 5: Send to user (This step is now handled by the main loop)
    
    Parameters
    ----------
    job : `Optional[Job]`
        The job to process.
    step : `int`
        The step the job is in.
    sq : `StepQueue`
        The step queue to process the jobs.
    """    
    print(f"Payloading Done Jobs {job.id_exec} for user {job.id_user} at {job.path} on step {step}, which is '{STEPS_CHART[step]}'")
    
    if step == 4:
        chatgpt_consolidation(job)
        sq.add_job(step + 1, job)
        sq.snapshot()
    # elif step == 5:
    #     status = await terminateJob(job)
    #     if not status:
    #         sq.addJob(step, job)
    #     sq.snapshot()

def generate_project_files(job: Job):
    """
    Generate the project files for the user and the exercise.

    Makes sure the fundamental directories are created and the exercise files are extracted.

    Warning: This function does not check if the user files are correct or if the exercise files are correct.
    Warning: As special format, only supports zip.
    
    Parameters
    ----------
    job : `Job`
        The job to process.
    """

    path = job.path

    user_path = os.path.join(path, str(job.id_user))
    exercise_path = os.path.join(path, "job_data")

    if not os.path.exists(user_path):
        os.makedirs(user_path)
    
    if not os.path.exists(exercise_path):
        os.makedirs(exercise_path)

    if db is None:
        job.broken = True
        return False
    
    ex_data = db.select("EXERCISE", {"id": job.id_exec, "type": job.category})[0]

    ex_id: int =           ex_data[0] # type: ignore
    ex_type: str =         ex_data[1] # type: ignore
    ex_title: str =        ex_data[2] # type: ignore
    ex_description: str =  ex_data[3] # type: ignore
    ex_difficulty:int =    ex_data[4] # type: ignore
    ex_content: str =      ex_data[5] # type: ignore
    ex_file: bytes =       ex_data[6] # type: ignore

    with open(os.path.join(exercise_path, f"{ex_id}.zip"), "wb") as f:
        f.write(ex_file)
    shutil.unpack_archive(os.path.join(exercise_path, f"{ex_id}.zip"), exercise_path)
    os.remove(os.path.join(exercise_path, f"{ex_id}.zip"))

    return True

    # Generate user files
    # zip_files = glob.glob(os.path.join(user_path, "**/*.zip"), recursive=True)
    # rar_files = glob.glob(os.path.join(user_path, "**/*.rar"), recursive=True)

    # for file in zip_files + rar_files:
    #     shutil.unpack_archive(file, user_path)
    #     os.remove(file)

def compilation_test(job: Job):
    """
    Test the compilation of the user files.

    Attempts to compile the user files and returns the result.
    Normally, the result matters when job.broken is True.
    
    Parameters
    ----------
    job : `Job`
        The job to process.
    
    Returns
    -------
    `tuple[bool, str]`
        A tuple with the status of the compilation and the result of the compilation.
    
    Raises
    ------
    `NotImplementedError`
        If the OS is not supported. (Should not happen, but just in case.)
    """
    path = os.path.join(job.path, str(job.id_user))

    java_files = glob.glob(os.path.join(path, "**/*.java"), recursive=True)

    if platform.system() == "Windows":
        command = ["cmd.exe", "/C"]
    elif platform.system() == "Linux":
        command = ["/bin/bash", "-c"]
    else:
        raise NotImplementedError("Unsupported OS")
    
    shutil.rmtree(os.path.join(path, "build"), ignore_errors=True)

    project_type = None

    if not java_files:
        return (False, "No Java files found for compilation.")
    
    directory = None

    if isant_project(path):
        command += ["ant"]
        file_path = glob.glob(os.path.join(path, "**/build.xml"), recursive=True)[0]
        directory = os.path.dirname(file_path)
        project_type = "Ant"
    elif ismaven_project(path):
        if platform.system() == "Windows":
            command += ["mvn", "verify"]
        else:
            command += ["mvn verify"]
        file_path = glob.glob(os.path.join(path, "**/pom.xml"), recursive=True)[0]
        directory = os.path.dirname(file_path)
        project_type = "Maven"
    elif len(java_files) == 1:
        build_path = os.path.join(path, "build")
        if platform.system() == "Windows":
            command += ["javac", "-d", build_path] + java_files
        else:
            command += [f"javac -d {build_path} {' '.join(java_files)}"]
        project_type = "Single file"
        job.java_file = java_files
    else:
        return (False, "No hemos podido determinar el tipo de proyecto.\nAsegúrate de que el proyecto sea de tipo Ant, Maven o un solo archivo Java.\n" +
                       "Si has subido un archivo comprimido, asegúrate de que el directorio raíz del proyecto sea el mismo que el del archivo comprimido.")
    
    job.project_type = project_type
    print(f"Compiling {project_type} project...")

    return compile_command(command, directory)

def isant_project(directory: str) -> bool:
    """
    Check if the directory is an Ant project. Simply checks if there are any build.xml files in the directory and its subdirectories.
    
    Parameters
    ----------
    directory : `str`
        The directory to check.
    
    Returns
    -------
    `bool`
        True if the directory is an Ant project, False otherwise.
    """
    build_files = glob.glob(os.path.join(directory, "**/build.xml"), recursive=True)

    if not build_files:
        return False
    return True

def ismaven_project(directory: str) -> bool:
    """
    Check if the directory is a Maven project. Simply checks if there are any pom.xml files in the directory or its subdirectories.
    
    Parameters
    ----------
    directory : `str`
        The directory to check.
    
    Returns
    -------
    `bool`
        True if the directory is a Maven project, False otherwise.
    """
    pom_files = glob.glob(os.path.join(directory, "**/pom.xml"), recursive=True)

    if not pom_files:
        return False
    return True

def compile_command(command: list[str], directory: Optional[str] = None) -> tuple[bool, str]:
    """
    Compile the user files with the given command.

    Parameters
    ----------
    command : `list[str]`
        The command to run. Has to include what shell to use.
    directory : `str, optional`
        The directory to run the command in. If not given, the current directory is used.
    
    Returns
    -------
    `tuple[bool, str]`
        A tuple with the status of the compilation and the result of the compilation.
    """
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=directory)
        return (True, result.stdout)
    except subprocess.CalledProcessError as e:
        return (False, e.stdout + e.stderr)

def running_test(job: Job) -> int:
    """
    Run the test cases for the user files.
    
    Parameters
    ----------
    job : `Job`
        The job to process.
    
    Returns
    -------
    `int`
        The status of the test cases. Where:
        0: All test cases passed.
        1: Some test cases failed.
        -1: Timeout.
    
    Raises
    ------
    `NotImplementedError`
        If the OS is not supported. (Should not happen, but just in case.)
    """
    
    exercise_path = os.path.join(job.path, "job_data")

    with open(os.path.join(exercise_path, "test_cases.json"), "r") as f:
        data = json.load(f)
    
    test_cases: list[dict[str, Any]] = data["test_cases"]

    if platform.system() == "Windows":
        command = ["cmd.exe", "/C"]
    elif platform.system() == "Linux":
        command = ["/bin/bash", "-c"]
    else:
        raise NotImplementedError("Unsupported OS")

    if job.project_type == "Ant":
        command += ["ant run"]
    elif job.project_type == "Maven":
        main_file = glob.glob(os.path.join(job.path, "**/Main.java"), recursive=True)
        if not main_file:
            job.broken = True
            job.text_content = (
                "No se ha encontrado el archivo Main.java."
                "Usando Maven, tu método main debe estar en la clase Main, en el archivo Main.java."
            )
            return 1
        main_file = main_file[0]
        main_file_split = main_file.split("\\" if platform.system() == "Windows" else "/")
        main_file = main_file_split[-2] + "." + main_file_split[-1].replace(".java", "")
        command += [f"mvn -q exec:java -Dexec.mainClass={main_file}"]
    elif job.project_type == "Single file":
        command += [f"java {job.java_file[0]}"]

    for i, test_case in enumerate(test_cases):
        status, result = run_test_case(test_case, command, job)

        if result is None:
            result = "No se ha obtenido resultado."

        result_truncated = result[-600:]
        if len(result) > 600:
            result_truncated = "...[hubo más output aquí arriba]...\n" + result_truncated

        test_input_text = "\n".join(str(x) for x in test_case["inputs"])
        test_output_text = "\n".join(str(x) for x in test_case["outputs"])

        if not status:
            job.broken = True
            if result_truncated == "Timeout":
                job.text_content = "El programa tardó demasiado en ejecutarse."
                return -1
            
            job.text_content = (
                f"Input dado: {test_input_text}\n"
                f"Output esperado: {test_output_text}\n"
                f"Output obtenido: {result_truncated}\n"
            )
            return 1
        
        obtained_lines = result.split("\n")
        if job.project_type == "Ant":
            obtained_lines = [line.removeprefix("[java]") for line in obtained_lines if line.strip().startswith("[java]")]

        if not compare_results(test_case["outputs"], obtained_lines):
            
            obtained_lines_text = "\n".join(obtained_lines) + "\n\n(Deberían haber tantas líneas de output obtenido casi como líneas de input dado y output esperado)"
            result_truncated = obtained_lines_text[-600:]
            if len(obtained_lines_text) > 600:
                result_truncated = "...[hubo más output aquí arriba]...\n" + result_truncated            

            job.broken = True
            job.text_content = (
                f"{i} pruebas superadas hasta el primer fallo.\n"
                f"Input dado: \n{test_input_text}\n"
                f"Output esperado: \n{test_output_text}\n"
                f"Output obtenido: \n{result_truncated}\n"
            )
            return 1
    
    if len(test_cases) == 0:
        job.text_content = "No hay pruebas que superar."
    else:
        job.text_content = f"Todos los test correctos, {len(test_cases)} en total."
    return 0

def run_test_case(test_case: dict, command: list[str], job: Job) -> tuple[bool, str]:
    """
    Run a test case with the given command.

    Uses subprocess to run the command and send the input to the process.
    Sends all the inputs in the test case to the process and then waits for the process to complete.
    
    Parameters
    ----------
    test_case : `dict`
        The test case to run.
    command : `list[str]`
        The command to run. Has to include what shell to use.
    job : `Job`
        The job to process.
    
    Returns
    -------
    `tuple[bool, str]`
        A tuple with the status of the test case and the result of the test case.
    """
   
    cwd = os.path.join(job.path, str(job.id_user))

    # Start the process
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd)

    if process.stdin is None:
        return (False, "No se ha podido ejecutar el proyecto. Error inesperado.")

    # Send input to the Ant project
    for input_data in test_case["inputs"]:
        input_data_bytes = bytes(str(input_data) + "\n", encoding="utf-8")
        process.stdin.write(input_data_bytes)
    # process.stdin.close()

    timeout = 60 if job.project_type == "Maven" else 30

    # Read and print the output of the Ant project
    try:
        # Read and print the output of the Ant project with a timeout
        output, output_err = process.communicate(timeout=timeout)
        if output is not None:
            output = output.decode("UTF-8", errors="replace")
        if output_err is not None:
            output_err = output_err.decode("UTF-8", errors="replace")

        # Wait for the process to complete
        exit_code = process.wait()

    except subprocess.TimeoutExpired:
        # Kill the process if it takes too long
        process.kill()
        print(f"Process killed due to timeout. Timeout: {timeout} seconds.")
        return (False, "Timeout")
    
    if exit_code != 0:
        return (False, output_err)
    else:
        return (True, output)

def compare_results(expected: list[str], obtained_lines: list[str]) -> bool:
    """
    Compare the expected results with the obtained results.

    For each expected result, it tries to find it in the obtained results.
    If found, it removes the result from the obtained results (making it unavailable for the next expected result).

    Warning: This function is case insensitive (not case sensitive).
    Warning: This function uses regular expressions to find the expected results in the obtained results.
    Warning: The obtained results could have incorrect results, but still pass this function.
    As it only checks if the expected results are in the obtained results, not if the obtained results are correct.
    This means that the obtained results could have more, arbitrary, results than the expected results, and still pass this function.

    Parameters
    ----------
    expected : `list[str]`
        The expected results.
    obtained_lines : `list[str]`
        The obtained results.
    
    Returns
    -------
    `bool`
        True if all the expected results were found in the obtained results, False otherwise.
    """
    obtained_text: str = "\n".join(obtained_lines)

    for i, line in enumerate(expected):
        line = str(line)
        found = re.search(fr"(?<!\w){re.escape(line)}(?!\w)", obtained_text, re.IGNORECASE)
        if found is None:
            return False

        target_index = found.start()
        
        obtained_text = obtained_text[:target_index] + obtained_text[target_index + len(line):]
    
    return True

def abstraction_test(job: Job) -> tuple[float, list[str]]:
    """
    An abstraction test to check the code for certain patterns defined in the abstraction.json file.
    
    Parameters
    ----------
    job : `Job`
        The job to process.
    
    Returns
    -------
    `tuple[float, list[str]]`
        A tuple with the score of the abstraction test and the banned patterns found.
        
        The score is a percentage of the required patterns found in the code.
        The banned patterns are the patterns found in the code that should not be there (does not affect the score).
    """
    exercise_path = os.path.join(job.path, "job_data")

    with open(os.path.join(exercise_path, "abstraction.json"), "r") as f:
        data: dict = json.load(f)
    
    required: dict[str, int] = data["required"]
    required_found: dict[str, int] = {}

    banned: list[str] = data["banned"]
    banned_found: list[str] = []

    total_code = str()
    java_files = glob.glob(os.path.join(job.path, "**/*.java"), recursive=True)
    for file in java_files:
        with open(file, "r", encoding="utf-8") as f:
            total_code += f.read()
    
    for key in required.keys():
        required_found[key] = len(re.findall(re.escape(key), total_code))
    
    for value in banned:
        if re.search(value, total_code):
            banned_found.append(value)

    required_total = sum(required.values())
    required_found_total = sum(required_found.values())

    return (((required_found_total / required_total) * 100) - 100, banned_found)

def chatgpt_consolidation(job: Job):
    """
    Consolidate the job with a ChatGPT message.

    Uses OpenAI's ChatGPT to generate a message based on the job's text content.
    Only intended to be used in case of a broken job.

    The result is stored in job.gpt_content, not returned.
    
    Parameters
    ----------
    job : `Job`
        The job to process.
    """
    try:
        client = OpenAI(api_key= os.environ.get("OPENAI_API_KEY"))
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "das consejo corto sobre este error"},
                {"role": "user", "content": (
                    f"{'hubo un error' if job.broken else 'todo bien'}\n"
                    f"Resultado: {job.text_content[-200:]}"
                    )
                }
            ]
        )
        if completion.choices[0].message.content:
            job.gpt_content = completion.choices[0].message.content
    
    except Exception as e:
        print(e)
        job.gpt_content = "No se pudo obtener respuesta de ChatGPT"   

async def updater(sq: StepQueue, key: str):
    """
    The updater function. This function listens for new jobs to be added to the queue.

    A listener thread waiting for new jobs, sended by the main DAW bot.
    
    Parameters
    ----------
    sq : `StepQueue`
        The step where the job is going to be added.
    key : `str`
        The key to authenticate the connection.
    """
    while True:
        print("Starting updater...")
        address = ('localhost', 6000)
        listener = Listener(address, authkey=key.encode('utf-8'))
        conn = listener.accept()
        print ('connection accepted from', listener.last_accepted)
        try:
            while True:
                print("Waiting for message...")
                msg = conn.recv()
                
                job = Job(msg['id_exec'], msg["category"], msg['id_user'], msg['path'])

                sq.add_job(msg['step'], job, msg['priority'])
        except ConnectionResetError:
            print("Connection reseted. Closing...")
            conn.close()
            listener.close()
        
        except EOFError:
            print("EOFError. Closing...")
            conn.close()
            listener.close()
        
        except Exception as e:
            print("Unexpected error. Closing...")
            print(e)
            conn.close()
            listener.close()

if __name__ == "__main__":
    terminal.initialize()

    try:
        db = Db()
    except Exception as e: # pylint: disable=broad-exception-caught
        error(e, traceback.format_exc(),
            "There was a problem connecting to the database, not loading database dependent cogs.",
            "Make sure the database credentials are loaded correctly.",
                level= "WARNING")
        db = None

    print("Starting bot...")

    asyncio.run(main())