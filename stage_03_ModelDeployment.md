3. Model Deployment and Inference
Truck delay application

2F3A302A-20F6-4947-8750-2137ECF30359

Step 1: Develop Streamlit applicaton (app.py) to create Truckdelay predictions as shown in above picture

connect to Hopsworks to get the final merge dataset

connect to mlflow model registry to get the model, encoder.pkl, scalar.pkl

write the python code to create streamlit application

        st.title('Truck Delay Classification')

        # Let's assume you have a list of options for your checkboxes
        options = ['date_filter', 'truck_id_filter', 'route_id_filter']
        
        # Use radio button to allow the user to select only one option for filtering
        selected_option = st.radio("Choose an option:", options)
        
        
        if selected_option == 'date_filter':
            st.write("### Date Ranges")
        
            #Date range
            from_date = st.date_input("Enter start date in YYYY-MM-DD : ", value=min(final_merge['departure_date']))
            to_date = st.date_input("Enter end date in YYYY-MM-DD : ", value=max(final_merge['departure_date']))
        
        elif selected_option == 'truck_id_filter':
        
            st.write("### Truck ID")
            truck_id = st.selectbox('Select truck ID: ', final_merge['truck_id'].unique())
            
        elif selected_option=='route_id_filter':
            st.write("### Route ID")
            route_id = st.selectbox('Select route ID: ', final_merge['route_id'].unique())
            
        if st.button('Predict'):
            try:
                flag = True
        
                if selected_option == 'date_filter':
                    sentence = "during the chosen date range"
                    filter_query = (final_merge['departure_date'] >= str(from_date)) & (final_merge['departure_date'] <= str(to_date))
                
                elif selected_option == 'truck_id_filter':
                    sentence = "for the specified truck ID"
                    filter_query = (final_merge['truck_id'] == truck_id)
            
                elif selected_option=='route_id_filter':
                    sentence = "for the specified route ID"
                    filter_query = (final_merge['route_id'] == str(route_id))
                
                else:
                    st.write("Please select at least one filter")
                    flag = False
