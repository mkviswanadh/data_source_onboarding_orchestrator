import os
import streamlit as st
from conversation_manager import ConversationManager

def main():
    st.set_page_config(page_title="Data Onboarding Chatbot")
    st.title("Autonomous Data Onboarding Chatbot")

    if "conv" not in st.session_state:
        st.session_state.conv = ConversationManager()

    if "history" not in st.session_state:
        st.session_state.history = []

    if "profiling_result" not in st.session_state:
        st.session_state.profiling_result = None

    user_input = st.text_input("You:", "")

    if st.button("Send") and user_input.strip():
        st.session_state.history.append({"role": "user", "content": user_input})
        reply = st.session_state.conv.user_message(user_input)
        st.session_state.history.append({"role": "assistant", "content": reply})

        # Example trigger for profiling
        if "profile table" in user_input.lower():
            profiling_result = st.session_state.conv.run_action(
                "profile_table",
                source="mysql_finance",
                schema="finance",
                table="transactions"
            )
            st.session_state.profiling_result = profiling_result
            st.success("Profiling completed.")

    # Display chat
    for msg in st.session_state.history:
        if msg["role"] == "user":
            st.markdown(f"**You:** {msg['content']}")
        else:
            st.markdown(f"**Bot:** {msg['content']}")

    # Add view report button if profiling is done
    if st.session_state.profiling_result:
        if st.button("View Detailed Profiling Report"):
            report_path = st.session_state.profiling_result.get("profile_report_path")
            if os.path.exists(report_path):
                with open(report_path, "r") as f:
                    html = f.read()
                st.components.v1.html(html, height=800, scrolling=True)
            else:
                st.error("Profiling report not found.")


if __name__ == "__main__":
    main()
