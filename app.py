import sys
import os
from sfn_blueprint import Task
from sfn_blueprint import SFNStreamlitView
from sfn_blueprint import SFNSessionManager
from sfn_blueprint import setup_logger
from sfn_blueprint import SFNDataPostProcessor
from agents.join_suggestions_agent import SFNJoinSuggestionsAgent
from views.streamlit_views import StreamlitView
from views.display_join_health import display_join_health
from utils.custom_data_loader import CustomDataLoader
import json
import pandas as pd

def run_app():
    # Initialize view and session
    view = StreamlitView(title="Data Join Advisor")
    session = SFNSessionManager()
    
    # Initialize current step if not exists
    if 'current_step' not in view.session_state:
        view.session_state.current_step = 1
    
    # Reset button
    col1, col2 = view.create_columns([7, 1])
    with col1:
        view.display_title()
    with col2:
        if view.display_button("🔄 Reset", key="reset_button",use_container_width=True):
            session.clear()
            view.session_state.current_step = 1
            view.session_state.file1_disabled = False
            view.session_state.file2_disabled = True
            if 'file1' in view.session_state:
                del view.session_state['file1']
            if 'file2' in view.session_state:
                del view.session_state['file2']
            view.rerun_script()

    # Step 1: Data Loading and Preview
    view.display_header("Step 1: Data Loading")
    view.display_markdown("---")
    
    # Initialize session states for file uploaders
    if 'file1_disabled' not in view.session_state:
        view.session_state.file1_disabled = False
    if 'file2_disabled' not in view.session_state:
        view.session_state.file2_disabled = True
    
    # Create two columns for file uploads
    col1, col2 = view.create_columns(2)
    
    with col1:
        view.display_subheader("Table 1")
        uploaded_file1 = view.file_uploader(
            "Choose first file", 
            key="file1",
            accepted_types=["csv", "xlsx", "json", "parquet"],
            disabled=view.session_state.file1_disabled
        )
        
        if uploaded_file1 is not None and not view.session_state.file1_disabled:
            with view.display_spinner('Loading first table...'):
                data_loader = CustomDataLoader()
                load_task1 = Task("Load first file", data=uploaded_file1)
                table1 = data_loader.execute_task(load_task1)
                session.set('table1', table1)
                view.session_state.file1_disabled = True
                view.session_state.file2_disabled = False
                view.rerun_script()
    
    with col2:
        view.display_subheader("Table 2")
        uploaded_file2 = view.file_uploader(
            "Choose second file", 
            key="file2",
            accepted_types=["csv", "xlsx", "json", "parquet"],
            disabled=view.session_state.file2_disabled
        )
        
        if uploaded_file2 is not None and not view.session_state.file2_disabled:
            with view.display_spinner('Loading second table...'):
                data_loader = CustomDataLoader()
                load_task2 = Task("Load second file", data=uploaded_file2)
                table2 = data_loader.execute_task(load_task2)
                session.set('table2', table2)
                view.session_state.file2_disabled = True
                view.rerun_script()

    # Show previews when both files are loaded
    if session.get('table1') is not None:
        view.show_message("✅ Table 1 uploaded successfully!", "success")
        
    if session.get('table1') is not None and session.get('table2') is not None:
        view.show_message("✅ Table 2 uploaded successfully!", "success")
        
        # Display data previews
        view.display_subheader("Data Preview")
        col1, col2 = view.create_columns(2)
        with col1:
            view.show_message("Table 1 Preview", "info")
            view.display_dataframe(session.get('table1').head())
        with col2:
            view.show_message("Table 2 Preview", "info")
            view.display_dataframe(session.get('table2').head())
            
        # Add proceed button only if we're still in Step 1
        if view.session_state.current_step == 1:
            if view.display_button("Proceed to Step 2", key="proceed_step2"):
                view.session_state.current_step = 2
                view.rerun_script()

    # Only show Step 2 if we're on step 2 and have both tables
    if view.session_state.current_step == 2 and session.get('table1') is not None and session.get('table2') is not None:
        # Setup logger
        logger, handler = setup_logger()
        
        # Step 2: Generate Join Suggestions
        view.display_header("Step 2: Join Analysis")
        view.display_markdown("---")

        if session.get('join_analysis') is None:
            with view.display_spinner('🤖 AI is analyzing possible join combinations...'):
                join_analyzer = SFNJoinSuggestionsAgent()
                analysis_task = Task("Analyze join possibilities", 
                                data={'table1': session.get('table1'),
                                     'table2': session.get('table2')})
                join_analysis = join_analyzer.execute_task(analysis_task)
                session.set('join_analysis', join_analysis)
                logger.info("Join analysis completed")
                view.rerun_script()  # Rerun to show results after analysis

        if session.get('join_analysis'):
            analysis = session.get('join_analysis')
            
            # Show number of suggestions found
            suggestion_count = analysis.get('suggestion_count', 0)
            if suggestion_count == 0 or not analysis.get('final_recommendations'):
                view.show_message("❌ Agent could not find valid join combination.", "error")
                view.show_message("""
                This could be due to: \n
                • Different date ranges in the tables \n
                • No matching customer IDs \n
                • Data format mismatches \n
                
                Please try manual column mapping for more control.""", "error")
            else:
                # Display join suggestions
                view.display_subheader("Available Join Options")  

                if suggestion_count == 1:
                    view.show_message("🎯 AI found a possible join strategy.", "info")
                else:
                    view.show_message(f"🎯 AI found {suggestion_count} possible join strategies.", "info")
                

                suggestion_data = analysis.get('initial_suggestions', {})
                
                if isinstance(suggestion_data, str):
                    try:
                        suggestion_data = json.loads(suggestion_data)
                    except json.JSONDecodeError:
                        view.show_message("❌ Error parsing suggestions", "error")
                        return

                # Display all suggestions in a single message
                suggestions_message = ""
                for suggestion_key, suggestion in suggestion_data.items():
                    suggestions_message += f" 📌 **Option {suggestion_key}**:\n\n"
                    suggestions_message += f"• **Date**: {suggestion.get('DateField', {}).get('table1')} ↔ {suggestion.get('DateField', {}).get('table2')}\n\n"
                    suggestions_message += f"• **Customer**: {suggestion.get('CustIDField', {}).get('table1')} ↔ {suggestion.get('CustIDField', {}).get('table2')}\n\n"
                    if suggestion.get('ProdID', {}).get('table1'):
                        suggestions_message += f"• **Product**: {suggestion.get('ProdID', {}).get('table1')} ↔ {suggestion.get('ProdID', {}).get('table2')}\n\n"
                    suggestions_message += " \n "
                
                view.show_message(suggestions_message.strip(), "info")

                # Display AI's recommendation
                view.display_markdown("---")
                view.display_subheader("AI Recommended Join Strategy")
                recommendation = analysis.get('final_recommendations', {})
                
                if 'recommended_join' in recommendation:
                    recommended_join = recommendation['recommended_join']
                    message = (
                        f"🎯 Recommended Join Fields:\n"
                        f"- Date: {recommended_join.get('date_mapping', {}).get('table1_field', 'N/A')} ↔ {recommended_join.get('date_mapping', {}).get('table2_field', 'N/A')}\n"
                        f"- Customer: {recommended_join.get('customer_mapping', {}).get('table1_field', 'N/A')} ↔ {recommended_join.get('customer_mapping', {}).get('table2_field', 'N/A')}"
                    )
                    
                    if (recommended_join.get('product_mapping') and 
                        recommended_join.get('product_mapping', {}).get('table1_field') and 
                        recommended_join.get('product_mapping', {}).get('table2_field')):
                        message += f"\n- Product: {recommended_join['product_mapping']['table1_field']} ↔ {recommended_join['product_mapping']['table2_field']}"
                    
                    view.show_message(message, "info")
                    
                    if 'explanation' in recommended_join:
                        view.show_message(f"📝 Reasoning:\n{recommended_join['explanation']}", "info")

                    # Add selection options
                    view.display_markdown("---")
                    join_choice = view.radio_select(
                        "How would you like to proceed?",
                        options=[
                            "Use AI Recommended Join Strategy",
                            "Select Columns Manually"
                        ],
                        key="join_choice", index=None
                    )

                    if join_choice == "Use AI Recommended Join Strategy":
                        selected_join = {
                            'customer_mapping': recommended_join['customer_mapping'],
                            'date_mapping': recommended_join['date_mapping']
                        }
                        if ('product_mapping' in recommended_join and 
                            recommended_join.get('product_mapping', {}).get('table1_field') and 
                            recommended_join.get('product_mapping', {}).get('table2_field')):
                            selected_join['product_mapping'] = recommended_join['product_mapping']
                        
                        # Store selected_join in session
                        session.set('selected_join', selected_join)
                        
                        # Add health check button for AI recommendation
                        if view.display_button("Check AI Recommendation Join Health", key='check_ai_health'):
                            table1 = session.get('table1')
                            table2 = session.get('table2')
                            join_analyzer = SFNJoinSuggestionsAgent()
                            # Get verification results instead of health check
                            verification_results = join_analyzer.check_join_health(
                                table1, 
                                table2, 
                                selected_join
                            )
                            display_join_health(verification_results, view, session)
                            session.set('join_health', verification_results)
                        
                        # Add Proceed to Join button if health check is done
                        if session.get('join_health'):
                            if view.display_button("Proceed to Join", key='proceed_to_join_ai'):
                                view.session_state.current_step = 3
                                view.rerun_script()

                    elif join_choice == "Select Columns Manually":
                        view.display_subheader("Manual Column Mapping")
                        
                        # Get tables from session
                        table1 = session.get('table1')
                        table2 = session.get('table2')
                        
                        # Create columns for side-by-side selection
                        col1, col2 = view.create_columns(2)
                        
                        with col1:
                            view.show_message("Table 1 Columns", "info")
                            table1_cols = [''] + list(table1.columns)
                            cust_id_col1 = view.select_box(
                                "Customer ID Column (Required)",
                                options=table1_cols,
                                key="cust_id_1"
                            )
                            
                            date_col1 = view.select_box(
                                "Date Column (Required)",
                                options=table1_cols,
                                key="date_1"
                            )
                            
                            # Validate date column 1 if selected
                            if date_col1 and date_col1 != '':
                                with view.display_spinner(f'Validating date format for {date_col1}...'):
                                    join_analyzer = SFNJoinSuggestionsAgent()
                                    try:
                                        # Try to normalize the date column
                                        normalized_dates = join_analyzer._normalize_date_column(table1, date_col1)
                                        if normalized_dates.isna().all():
                                            view.show_message(f"❌ No valid dates found in column '{date_col1}'", "error")
                                            date_col1 = None
                                    except Exception as e:
                                        view.show_message(f"❌ '{date_col1}' is not a valid date field: {str(e)}", "error")
                                        sample_values = table1[date_col1].dropna().head(5).tolist()
                                        view.show_message(f"Sample values: {sample_values}", "info")
                                        date_col1 = None
                            
                            prod_col1 = view.select_box(
                                "Product Column (Optional)",
                                options=['None'] + list(table1.columns),
                                key="prod_1"
                            )
                        
                        with col2:
                            view.show_message("Table 2 Columns", "info")
                            table2_cols = [''] + list(table2.columns)
                            cust_id_col2 = view.select_box(
                                "Customer ID Column (Required)",
                                options=table2_cols,
                                key="cust_id_2"
                            )
                            
                            date_col2 = view.select_box(
                                "Date Column (Required)",
                                options=table2_cols,
                                key="date_2"
                            )
                            
                            # Validate date column 2 if selected
                            if date_col2 and date_col2 != '':
                                with view.display_spinner(f'Validating date format for {date_col2}...'):
                                    join_analyzer = SFNJoinSuggestionsAgent()
                                    try:
                                        # Try to normalize the date column
                                        normalized_dates = join_analyzer._normalize_date_column(table2, date_col2)
                                        if normalized_dates.isna().all():
                                            view.show_message(f"❌ No valid dates found in column '{date_col2}'", "error")
                                            date_col2 = None
                                    except Exception as e:
                                        view.show_message(f"❌ '{date_col2}' is not a valid date field: {str(e)}", "error")
                                        sample_values = table2[date_col2].dropna().head(5).tolist()
                                        view.show_message(f"Sample values: {sample_values}", "info")
                                        date_col2 = None
                            
                            prod_col2 = view.select_box(
                                "Product Column (Optional)",
                                options=['None'] + list(table2.columns),
                                key="prod_2"
                            )

                        # Create mapping configuration
                        if all([cust_id_col1, cust_id_col2, date_col1, date_col2]):  # Required fields selected
                            selected_join = {
                                'customer_mapping': {
                                    'table1_field': cust_id_col1,
                                    'table2_field': cust_id_col2
                                },
                                'date_mapping': {
                                    'table1_field': date_col1,
                                    'table2_field': date_col2
                                }
                            }
                            
                            # Add product mapping only if both product columns are selected and not 'None'
                            if prod_col1 != 'None' and prod_col2 != 'None':
                                selected_join['product_mapping'] = {
                                    'table1_field': prod_col1,
                                    'table2_field': prod_col2
                                }
                            
                            # Store selected_join in session
                            session.set('selected_join', selected_join)
                            
                            if view.display_button("Check Join Health", key='check_manual_health'):
                                join_analyzer = SFNJoinSuggestionsAgent()
                                # Get verification results instead of health check
                                verification_results = join_analyzer.check_join_health(
                                    table1, 
                                    table2, 
                                    selected_join
                                )
                                display_join_health(verification_results, view, session)
                                session.set('join_health', verification_results)
                            
                            # Add Proceed to Join button if health check is done
                            if session.get('join_health'):
                                if view.display_button("Proceed to Join", key='proceed_to_join_manual'):
                                    view.session_state.current_step = 3
                                    view.rerun_script()

    # Only show Step 3 if we're on step 3 and have all required data
    if view.session_state.current_step == 3 and session.get('selected_join') and session.get('join_health'):
        # Get selected_join from session
        selected_join = session.get('selected_join')

        # Show Step 2 header and finalized join strategy
        view.display_markdown("---")
        view.display_header("Step 2: Join Analysis")
        
        view.show_message(
            "✅ Finalized Join Fields:\n\n"
            f"• Date: {selected_join['date_mapping']['table1_field']} ↔ {selected_join['date_mapping']['table2_field']}\n\n"
            f"• Customer: {selected_join['customer_mapping']['table1_field']} ↔ {selected_join['customer_mapping']['table2_field']}\n\n"
            + (f"\n• Product: {selected_join['product_mapping']['table1_field']} ↔ {selected_join['product_mapping']['table2_field']}\n\n"
               if 'product_mapping' in selected_join else ""),
            "success"
        )
        
        view.display_markdown("---")

        # Step 3 header and join operation
        view.display_header("Step 3: Join Results")

        # Perform the join if not already done
        if session.get('joined_data') is None:
            with view.display_spinner('Performing join operation...'):
                try:
                    table1 = session.get('table1')
                    table2 = session.get('table2')
                    join_analyzer = SFNJoinSuggestionsAgent()
                    
                    # Normalize date columns before join
                    date_col1 = selected_join['date_mapping']['table1_field']
                    date_col2 = selected_join['date_mapping']['table2_field']
                    
                    # Create copies to avoid modifying original dataframes
                    table1_copy = table1.copy()
                    table2_copy = table2.copy()
                    
                    # Normalize dates and create new columns for joining
                    table1_copy['normalized_date'] = join_analyzer._normalize_date_column(table1_copy, date_col1)
                    table2_copy['normalized_date'] = join_analyzer._normalize_date_column(table2_copy, date_col2)
                    
                    # Create merge conditions
                    merge_conditions = [
                        (selected_join['customer_mapping']['table1_field'], 
                         selected_join['customer_mapping']['table2_field']),
                        ('normalized_date', 'normalized_date')  # Use normalized date columns
                    ]
                    
                    if 'product_mapping' in selected_join:
                        merge_conditions.append(
                            (selected_join['product_mapping']['table1_field'], 
                             selected_join['product_mapping']['table2_field'])
                        )
                    
                    # Perform merge
                    joined_df = table1_copy.merge(
                        table2_copy,
                        left_on=[m[0] for m in merge_conditions],
                        right_on=[m[1] for m in merge_conditions],
                        how='inner'
                    )
                    
                    # Drop the normalized date column as it was only needed for joining
                    joined_df = joined_df.drop(['normalized_date'], axis=1)
                    
                    session.set('joined_data', joined_df)
                    view.rerun_script()
                    
                except Exception as e:
                    view.show_message(f"❌ Error performing join: {str(e)}", "error")
                    return

        # Show join results
        if session.get('joined_data') is not None:
            joined_df = session.get('joined_data')
            
            # Show join statistics

            view.show_message(
                f"✅ Join completed successfully!\n\n"
                f"• Total rows in joined data: {len(joined_df):,}\n\n"
                f"• Total columns: {len(joined_df.columns):,}",
                "success"
            )

            view.display_markdown("---")
            view.display_header("Step 4: Post Processing")
            
            # Add post-processing options
            operation_type = view.radio_select(
                "Choose an operation:",
                ["View Joined Data", "Download Joined Data", "Finish"],
                key="post_processing_choice"
            )

            if operation_type == "View Joined Data":
                view.display_subheader("Preview of Joined Data")
                view.display_dataframe(joined_df.head())
                
            elif operation_type == "Download Joined Data":
                if view.display_button("Download CSV", key="download_csv"):
                    view.create_download_button(
                        "Download CSV",
                        joined_df.to_csv(index=False),
                        "joined_data.csv",
                        "text/csv"
                    )
            
            elif operation_type == "Finish":
                view.show_message("🎉 Thank you for using the Data Join Advisor!")
                if view.display_button("Confirm finish", key="finish"):
                    session.clear()
                    view.session_state.current_step = 1
                    view.session_state.file1_disabled = False
                    view.session_state.file2_disabled = True
                    if 'file1' in view.session_state:
                        del view.session_state['file1']
                    if 'file2' in view.session_state:
                        del view.session_state['file2']
                    view.rerun_script()

if __name__ == "__main__":        
    run_app()