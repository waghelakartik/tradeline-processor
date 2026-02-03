import streamlit as st
import pandas as pd
import time
import os
import re  # Added for Regex Extraction
# Import the processor logic
import process_experian

# ================================
# PAGE CONFIG
# ================================
st.set_page_config(
    page_title="Tradeline Processor",
    page_icon="📊",
    layout="wide"
)

# Custom CSS for modern look
st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        height: 3em;
        font-weight: bold;
    }
    .status-box {
        padding: 10px;
        background-color: #f0f2f6;
        border-radius: 5px;
        margin-bottom: 10px;
        font-family: monospace;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Session State
if 'processing' not in st.session_state:
    st.session_state['processing'] = False

# ================================
# HEADER
# ================================
st.title("📊 Tradeline Processor")
st.markdown("High-performance parallel processing system for credit reports.")
st.markdown("---")

# ================================
# SIDEBAR SETTINGS
# ================================
with st.sidebar:
    st.header("⚙️ Settings")
    max_workers = st.slider("Concurrent Threads", min_value=1, max_value=50, value=20, step=1)
    st.info(f"Currently configured to process **{max_workers}** reports simultaneously.")
    
    st.divider()
    
    st.header("📝 Filter Options")
    st.markdown("Enter PANs below. You can copy-paste lists, bullets, or piles of text. The system will auto-extract valid PANs.")
    pan_input = st.text_area("Specific PANs", height=200, placeholder="• ABCDE1234F\n- FGHIJ5678K\nOr just paste an email...")

# ================================
# MAIN INTERFACE
# ================================

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Control Panel")
    
    # Logic for Buttons based on State
    if not st.session_state['processing']:
        start_btn = st.button("🚀 START PROCESSING", type="primary")
        stop_btn = st.button("🛑 STOP / CANCEL", disabled=True)
    else:
        start_btn = st.button("🚀 PROCESSING...", disabled=True)
        stop_btn = st.button("🛑 STOP / CANCEL", type="secondary")

    if stop_btn:
        st.session_state['processing'] = False
        st.experimental_rerun()

    st.markdown("### Output")
    output_placeholder = st.empty()

with col2:
    st.subheader("Live Status")
    progress_bar = st.progress(0)
    status_text = st.empty()
    log_area = st.empty()

# ================================
# LOGIC
# ================================

if start_btn:
    st.session_state['processing'] = True
    
    # 1. Parse PAN Input (REGEX MODE)
    specific_pans = []
    if pan_input:
        # Regex to find standard Indian PAN pattern: 5 Letters, 4 Digits, 1 Letter
        # Finds matches anywhere in the text (ignoring bullets, commas, sentences, etc)
        pattern = r'[A-Za-z]{5}\d{4}[A-Za-z]{1}'
        matches = re.findall(pattern, pan_input)
        
        # Unique and Uppercase
        specific_pans = sorted(list(set([m.upper() for m in matches])))
        
    if specific_pans:
        st.toast(f"✅ Extracted {len(specific_pans)} Valid PANs")
        st.info(f"Processing {len(specific_pans)} filtered PANs: {', '.join(specific_pans[:5])}...")
    else:
        if pan_input and len(pan_input.strip()) > 0:
            st.warning("⚠️ Text entered but NO valid PANs found! Processing ALL records instead?")
            # Decide: Should we stop? Safe bet: If user entered text but no PANs found, likely typo.
            # But currently logic falls back to ALL. Let's make it strict if input exists.
            st.error("Text entered but no valid PAN patterns (ABCDE1234F) found. Please check input.")
            st.session_state['processing'] = False
            st.stop()
        else:
             st.toast("Processing ALL records from Database")

    # 2. Callback Function for UI Updates
    def update_ui(current, total, message):
        # Update Status Text
        status_text.markdown(f"**Status:** {message}")
        
        # Update Progress Bar
        if total > 0:
            percent = min(current / total, 1.0)
            progress_bar.progress(percent)
        else:
            progress_bar.progress(0)
            
    # 3. Run Processor
    try:
        with st.spinner("Processing started... Please wait."):
            # Check if stopped before running (basic check)
            if not st.session_state['processing']:
                st.stop()
                
            df = process_experian.run_processor(
                max_workers=max_workers, 
                specific_pans=specific_pans, 
                progress_callback=update_ui
            )
        
        # 4. Handle Completion
        if df is not None and not df.empty:
            st.success("✅ Processing Complete!")
            status_text.markdown("**Status:** Job Finished Successfully.")
            progress_bar.progress(100)
            
            # Show Preview
            with st.expander("📄 Data Preview (First 50 Rows)", expanded=True):
                st.dataframe(df.head(50))
            
            # Download Button
            csv = df.to_csv(index=False).encode('utf-8')
            output_placeholder.download_button(
                label="📥 Download Excel/CSV Data",
                data=csv,
                file_name="processed_trade_lines.csv",
                mime="text/csv",
            )
            
            excel_path = process_experian.OUTPUT_FILE
            if os.path.exists(excel_path):
                with open(excel_path, "rb") as f:
                    output_placeholder.download_button(
                        label="📥 Download Excel (.xlsx)",
                        data=f,
                        file_name="processed_trade_lines.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        elif df is None:
             st.warning("Job stopped or returned no data.")
        else:
            st.error("Processing finished but returned no data. Check inputs.")
            
    except Exception as e:
        st.error(f"An error occurred: {e}")
    
    # Reset State after completion
    st.session_state['processing'] = False
