import {
  getFirestore,
  doc,
  getDoc,
} from "https://www.gstatic.com/firebasejs/11.0.1/firebase-firestore.js";
import { firebaseInit } from "./firebaseConfig.js"; // Adjust the path as needed

class DashboardUI {
  constructor() {
    this.db = null;
    this.currentMetrics = null;
    this.initialize();
  }

  async initialize() {
    const { db } = await firebaseInit; // Wait for Firebase to initialize
    this.db = db;

    this.setupEventListeners();
    await this.loadData();
  }

  setupEventListeners() {
    document
      .getElementById("countryFilter")
      .addEventListener("change", () => this.updateDashboard());
  }

  async loadData() {
    const docRef = doc(this.db, "processed_analytics", "latest");
    const docSnap = await getDoc(docRef);

    if (docSnap.exists()) {
      // Try to get the data in the expected format
      const docData = docSnap.data();
      console.log("Raw Firestore data:", docData); // Debug data structure

      // Check if we have the new nested format (data.data) or just data directly
      let rawData = docData.data;

      // Check which data structure we have and normalize it
      if (rawData) {
        console.log("Raw data structure:", rawData);

        // If we already have the expected format with 'all' and 'by_country'
        if (rawData.all && rawData.by_country) {
          this.currentMetrics = rawData;
        }
        // If we have just country data without the wrapper
        else if (
          !rawData.all &&
          Object.keys(rawData).some((key) => rawData[key].daily)
        ) {
          // Create the expected structure
          this.currentMetrics = {
            all: this._aggregateAllCountries(rawData),
            by_country: rawData,
          };
        }
        // Something else entirely
        else {
          console.error("Unexpected data format:", rawData);
          this.showError("Data format error. Please check the console.");
          return;
        }

        console.log("Processed metrics:", this.currentMetrics);
        this.updateDashboard();
      } else {
        console.error("No data found in the document");
        this.showError("No data available");
      }
    } else {
      console.error("Document not found");
      this.showError("Analytics data not found");
    }
  }

  // Helper method to aggregate country data in the frontend if needed
  _aggregateAllCountries(countryData) {
    console.log("Aggregating country data on frontend");
    const allMetrics = { daily: {} };

    // Combine data from all countries
    Object.values(countryData).forEach((country) => {
      if (!country.daily) return;

      Object.entries(country.daily).forEach(([date, metrics]) => {
        if (!allMetrics.daily[date]) {
          allMetrics.daily[date] = {
            pageviews: 0,
            unique_visitors: 0,
            total_time: 0,
          };
        }

        allMetrics.daily[date].pageviews += metrics.pageviews;
        allMetrics.daily[date].unique_visitors += metrics.unique_visitors;
        allMetrics.daily[date].total_time += metrics.total_time;
      });
    });

    return allMetrics;
  }

  // Add this helper method
  showError(message) {
    const container = document.getElementById("dashboard-container");
    container.innerHTML = `<div class="error-message">${message}</div>`;
  }

  updateDashboard() {
    const country = document.getElementById("countryFilter").value;
    const data =
      country === "all"
        ? this.currentMetrics.all
        : this.currentMetrics.by_country[country];

    // Get yesterday's date
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split("T")[0];

    // Update the cutoff date display
    document.getElementById("cutoffDate").innerHTML = `
      <p class="text-gray-600 text-sm">Data shown through: ${new Date(
        yesterdayStr
      ).toLocaleDateString()}</p>
    `;

    this.updateMetricCards(data, yesterdayStr);
    this.updateCharts(data);
  }

  updateMetricCards(data, yesterdayStr) {
    // Get yesterday's metrics
    const yesterdayMetrics = data.daily[yesterdayStr] || {
      pageviews: 0,
      unique_visitors: 0,
      total_time: 0,
    };

    // Calculate both averages
    const avgTimePerPageview = Math.round(
      yesterdayMetrics.total_time / yesterdayMetrics.pageviews || 0
    );
    const avgTimePerVisitor = Math.round(
      yesterdayMetrics.total_time / yesterdayMetrics.unique_visitors || 0
    );

    document.getElementById("pageviews").innerHTML = `
      <h3 class="text-lg font-semibold mb-2">Pageviews</h3>
      <p class="text-3xl">${yesterdayMetrics.pageviews}</p>
    `;

    document.getElementById("visitors").innerHTML = `
      <h3 class="text-lg font-semibold mb-2">Unique Visitors</h3>
      <p class="text-3xl">${yesterdayMetrics.unique_visitors}</p>
    `;

    document.getElementById("timeOnSite").innerHTML = `
      <h3 class="text-lg font-semibold mb-2">Avg Time on Site</h3>
      <div class="space-y-1">
        <p class="text-xl">Per pageview: ${avgTimePerPageview}s</p>
        <p class="text-xl">Per visitor: ${avgTimePerVisitor}s</p>
      </div>
    `;
  }

  updateCharts(data) {
    // Get yesterday's date
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const yesterdayStr = yesterday.toISOString().split("T")[0];

    // Filter and sort dates up to yesterday
    const dates = Object.keys(data.daily)
      .filter((date) => date <= yesterdayStr)
      .sort();

    const pageviews = dates.map((date) => data.daily[date].pageviews);
    const uniqueVisitors = dates.map(
      (date) => data.daily[date].unique_visitors
    );
    const avgTimePerPageview = dates.map((date) =>
      Math.round(data.daily[date].total_time / data.daily[date].pageviews)
    );
    const avgTimePerVisitor = dates.map((date) =>
      Math.round(data.daily[date].total_time / data.daily[date].unique_visitors)
    );

    // Common layout and config settings
    const layout = {
      autosize: true,
      margin: { l: 40, r: 10, t: 30, b: 40 },
      responsive: true,
      showlegend: false,
    };

    const config = {
      displayModeBar: false,
      responsive: true,
    };

    // Trend Chart for Pageviews
    Plotly.newPlot(
      "trendChart",
      [
        {
          x: dates,
          y: pageviews,
          type: "scatter",
          mode: "lines+markers",
          name: "Pageviews",
          line: { shape: "linear" },
        },
      ],
      {
        ...layout,
        title: {
          text: "Daily Pageviews",
          y: 0.95,
        },
      },
      config
    );

    // Unique Visitors Chart
    Plotly.newPlot(
      "uniqueVisitorsChart",
      [
        {
          x: dates,
          y: uniqueVisitors,
          type: "scatter",
          mode: "lines+markers",
          name: "Unique Visitors",
          line: { shape: "linear" },
        },
      ],
      {
        ...layout,
        title: {
          text: "Daily Unique Visitors",
          y: 0.95,
        },
      },
      config
    );

    // Avg Time Per Pageview Chart
    Plotly.newPlot(
      "avgTimePerPageviewChart",
      [
        {
          x: dates,
          y: avgTimePerPageview,
          type: "scatter",
          mode: "lines+markers",
          name: "Avg Time/Pageview",
          line: { shape: "linear" },
        },
      ],
      {
        ...layout,
        title: {
          text: "Average Time per Pageview (seconds)",
          y: 0.95,
        },
      },
      config
    );

    // Avg Time Per Visitor Chart
    Plotly.newPlot(
      "avgTimePerVisitorChart",
      [
        {
          x: dates,
          y: avgTimePerVisitor,
          type: "scatter",
          mode: "lines+markers",
          name: "Avg Time/Visitor",
          line: { shape: "linear" },
        },
      ],
      {
        ...layout,
        title: {
          text: "Average Time per Visitor (seconds)",
          y: 0.95,
        },
      },
      config
    );

    // Growth Chart
    const growthDates = Object.keys(data.growth).filter(
      (date) => date <= yesterdayStr
    );
    const growthValues = growthDates.map((date) => data.growth[date]);

    Plotly.newPlot(
      "growthChart",
      [
        {
          x: growthDates,
          y: growthValues,
          type: "bar",
          name: "Growth %",
        },
      ],
      {
        ...layout,
        title: {
          text: "Rolling Growth Rate (%)",
          y: 0.95,
        },
      },
      config
    );
  }
}

new DashboardUI();
