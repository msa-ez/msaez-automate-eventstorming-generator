import os
from dotenv import load_dotenv
load_dotenv()

from eventstorming_generator.terminal import command_handlers
from eventstorming_generator.utils import ListUtil

previous_command = None
def terminal():
    """
    터미널 인터페이스를 통해 로직을 실행
    """
    
    if not os.path.exists(".temp"):
        os.makedirs(".temp")

    while True:
        print()
        user_input = input(">>> ")
        if user_input == "exit":
            break
        if not user_input:
            continue
        if user_input == "r" and previous_command:
            user_input = previous_command

        command_parts = user_input.split(' ')
        command_type = ListUtil.get_safely(command_parts, 0, None)
        command_name = ListUtil.get_safely(command_parts, 1, None)
        command_args = command_parts[2:] if len(command_parts) > 2 else []

        handler = command_handlers.get(command_type, None)
        if handler:
            try:
                handler(command_name, command_args)
            except Exception as e:
                print(f"로직 실행 중 오류 발생: {e}")
        else:
            print("유효하지 않은 콘솔 명령어입니다.\n사용 가능한 명령어를 보려면 'help' 명령어를 입력하세요.")
        previous_command = user_input

if __name__ == "__main__":
    terminal()