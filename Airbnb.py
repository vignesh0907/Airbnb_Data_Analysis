from pymongo import MongoClient
from bson import Decimal128, ObjectId
import streamlit as st
import pandas as pd
import plotly.express as px

@st.cache_data(ttl=600)
def load_data():
    username = "Enter your username"
    password = "Enter your password"
    cluster_url = "Enter your URL"
    database_name = "sample_airbnb"
    collection_name = "listAndReviews"

    client = MongoClient(f"mongodb+srv://username:password@cluster url/sample_airbnb?ssl=true&ssl_cert_reqs=CERT_NONE")
    db = client['sample_airbnb']
    collection = db['listingsAndReviews']

    transformed_data = []
    for document in collection.find():
        transformed_doc = {
            "_id": str(document.get("_id")),  
            "name": document.get("name"),
            "description": document.get("description"),
            "host_id": document.get("host", {}).get("host_id"),
            "host_name": document.get("host", {}).get("host_name"),
            "host_listing_count": document.get("host", {}).get("host_total_listings_count"),
            "property_type": document.get("property_type"),
            "room_type": document.get("room_type"),
            "minimum_nights": document.get("minimum_nights"),
            "maximum_nights": document.get("maximum_nights"),
            "neighbourhood": document.get("address").get("suburb"),
            "location": {
                "type": "Point",
                "longitude": document.get("address").get("location").get("coordinates")[0],
                "latitude": document.get("address").get("location").get("coordinates")[1]
            },
            "price": float(document.get("price", Decimal128("0")).to_decimal()),  
            "availability": document.get("availability").get("availability_365"),
            # {
                # "availability_30": document.get("availability").get("availability_30"),
                # "availability_60": document.get("availability").get("availability_60"),
                # "availability_90": document.get("availability").get("availability_90"),
                # "availability_365": document.get("availability").get("availability_365")
            # },
            "amenities": document.get("amenities"),
            "rating": float(document.get("review_scores").get("review_scores_rating", 0)),
            # {
                # "review_scores_accuracy": float(document.get("review_scores").get("review_scores_accuracy", 0)),
                # "review_scores_cleanliness": float(document.get("review_scores").get("review_scores_cleanliness", 0)),
                # "review_scores_checkin": float(document.get("review_scores").get("review_scores_checkin", 0)),
                # "review_scores_communication": float(document.get("review_scores").get("review_scores_communication", 0)),
                # "review_scores_location": float(document.get("review_scores").get("review_scores_location", 0)),
                # "review_scores_value": float(document.get("review_scores").get("review_scores_value", 0)),
                # "review_scores_rating": float(document.get("review_scores").get("review_scores_rating", 0))
            # },
            "last_review_date": str(document.get("last_review")).split('T')[0],
            "reviews": [
                {
                    "reviewer_id": str(review.get("_id")),  # Convert ObjectId to string
                    "reviewer_name": review.get("reviewer_name"),
                    "comment": review.get("comments"),
                    "rating": float(review.get("rating", 0))  # Convert Decimal128 to float
                }
                for review in document.get("reviews")
            ],
            "transit": document.get("transit")
        }
        total_reviews_count = len(document.get("reviews"))
        transformed_doc["reviews_count"] = total_reviews_count
        transformed_data.append(transformed_doc)

    df = pd.DataFrame(transformed_data)
    # Fill null values
    df['last_review_date'] = pd.to_datetime(df['last_review_date'], errors='coerce')
    df = df.dropna(subset=['last_review_date'])
    df['last_review_date'] = df['last_review_date'].dt.date


    df['neighbourhood'] = df['neighbourhood'].replace('', None)
    df = df.dropna(subset=['neighbourhood'])
    df['longitude'] = df['location'].apply(lambda x: x['longitude'])
    df['latitude'] = df['location'].apply(lambda x: x['latitude'])
    df = df.drop(columns=['location'])

    df.to_csv('Transformed_Data.csv', index=False)

    return df
def display_map_on_streamlit(df, selected_neighbourhood, selected_property_type):
    filtered_df = df[(df['neighbourhood'] == selected_neighbourhood) & (df['property_type'] == selected_property_type)]
    filtered_df['popup_info'] = (
        'Host ID: ' + filtered_df['host_id'].astype(str) + '<br>' +
        'Room Type: ' + filtered_df['room_type'] + '<br>' +
        'Price: $' + filtered_df['price'].astype(str) + '<br>' +
        'Availability: ' + filtered_df['availability'].astype(str) + '<br>' +
        'Rating: ' + filtered_df['rating'].astype(str)
    )


    fig = px.scatter_mapbox(
        filtered_df,
        lat='latitude',
        lon='longitude',
        hover_name='neighbourhood',  
        hover_data={'neighbourhood': True, 'popup_info': True},
        zoom=10,
    )

    # Customize the map layout
    fig.update_layout(
        mapbox_style="carto-positron", #carto-positron
        mapbox_zoom=10,
        mapbox_center={'lat': filtered_df['latitude'].mean(), 'lon': filtered_df['longitude'].mean()},
    )

    # Show the map in Streamlit
    st.plotly_chart(fig)

def price_analysis(df, selected_neighbourhood, selected_property_type):
    st.header('Price Analysis and Visualization')
    st.write("Before Selecting the Neighbourhood the Graph is displayed based on all the neighbourhood's and selected Property Type")
    if selected_neighbourhood is not None:
        filtered_df = df[(df['neighbourhood'] == selected_neighbourhood) & (df['property_type'] == selected_property_type)]
        filtered_df_neighbourhood = df[df['neighbourhood'] == selected_neighbourhood]
        if not filtered_df.empty:
            fig_price_distribution = px.histogram(filtered_df, x='price', nbins=30, title=f'Price Distribution for {selected_neighbourhood}, {selected_property_type}')
            st.plotly_chart(fig_price_distribution)

            fig_price_distribution_room_type = px.histogram(filtered_df_neighbourhood, y='price', x='room_type', nbins=30, title=f'Price Distribution by Room Type for {selected_neighbourhood}')
            st.plotly_chart(fig_price_distribution_room_type)

            property_avg_price = df[df['neighbourhood'] == selected_neighbourhood].groupby('property_type')['price'].mean().reset_index()
            fig_price_variations_pie = px.pie(property_avg_price, values='price', names='property_type', title=f'Average Price Distribution by Property Type for {selected_neighbourhood}')
            st.plotly_chart(fig_price_variations_pie)
        else:
            st.write('No data available for the selected filters.')
    else:
        st.write("Please select a neighborhood to view the price distribution and variations.")

def main():
    st.set_page_config(layout='wide')
    st.title('Airbnb Project')

    tab_selection = st.sidebar.selectbox("Select a tab:", ['Data Extraction', 'Explore Data'])

    df = None

    if tab_selection == 'Data Extraction':
        st.header('Get Data from MongoDB Atlas')
        st.write('To retrive the data from MongoDB Atlas click on the below button. Please ensure MongoDB Atlas Account is active right now..!')
        data_extract_btn = st.button('Get Data')
        if data_extract_btn:
            df = load_data()
            st.dataframe(df)
            st.success('Data Extracted Successfully')

    elif tab_selection == 'Explore Data':
        st.header('Explore the Data on Map')
        st.write('We can explore the data visually in Geographical Map. Please select a Neighbourhood')
        df = pd.read_csv('Transformed_Data.csv')
        if df is not None:
            selected_neighbourhood = st.selectbox('Select Neighbourhood', df['neighbourhood'].unique())
            selected_property_type = st.selectbox('Select Property Type', df[df['neighbourhood'] == selected_neighbourhood]['property_type'].unique())
            explore_data_btn = st.button('Explore in Map')
            if explore_data_btn:
                display_map_on_streamlit(df, selected_neighbourhood, selected_property_type)

            price_analysis(df, selected_neighbourhood, selected_property_type)  

if __name__ == "__main__":
    main()
