def main(repetition, strength):
    import sys
    print(f"I am called with repetition={repetition}, strength={strength}")


# hide executions that you only want to execute when the script is run directly behind if __name__ == "__main__"
if __name__ == "__main__":
    # execute it if you call the file directly
    main(1, 0.3)
