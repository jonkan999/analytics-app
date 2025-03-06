import {
  getFirestore,
  doc,
  getDoc,
} from "https://www.gstatic.com/firebasejs/11.0.1/firebase-firestore.js";
import { firebaseInit } from "./firebaseConfig.js"; // Adjust the path as needed

// Simply access Plotly directly from window
const Plotly = window.Plotly;

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

    // First collect all dates
    const allDates = new Set();
    Object.values(countryData).forEach((country) => {
      if (!country.daily) return;
      Object.keys(country.daily).forEach((date) => allDates.add(date));
    });

    // Initialize all dates with zero values
    allDates.forEach((date) => {
      allMetrics.daily[date] = {
        pageviews: 0,
        rolling_7: 0,
        rolling_28: 0,
        growth_7: 0,
        growth_28: 0,
      };
    });

    // Combine data from all countries
    Object.values(countryData).forEach((country) => {
      if (!country.daily) return;

      Object.entries(country.daily).forEach(([date, metrics]) => {
        allMetrics.daily[date].pageviews += metrics.pageviews || 0;

        // Copy rolling metrics if available (might be calculated by backend)
        if (metrics.rolling_7)
          allMetrics.daily[date].rolling_7 += metrics.rolling_7;
        if (metrics.rolling_28)
          allMetrics.daily[date].rolling_28 += metrics.rolling_28;
      });
    });

    // Calculate rolling metrics if not provided by backend
    const datesSorted = Object.keys(allMetrics.daily).sort();
    if (!allMetrics.daily[datesSorted[0]].rolling_7) {
      this._calculateRollingMetrics(allMetrics, datesSorted);
    }

    return allMetrics;
  }

  // Calculate rolling metrics if not provided by backend
  _calculateRollingMetrics(metrics, datesSorted) {
    for (let i = 0; i < datesSorted.length; i++) {
      const date = datesSorted[i];

      // Calculate 7-day rolling if we have enough data
      if (i >= 6) {
        let sum7 = 0;
        for (let j = i - 6; j <= i; j++) {
          sum7 += metrics.daily[datesSorted[j]].pageviews;
        }
        metrics.daily[date].rolling_7 = sum7;

        // Calculate 7-day growth if we have enough data
        if (i >= 13) {
          let previousSum7 = 0;
          for (let j = i - 13; j <= i - 7; j++) {
            previousSum7 += metrics.daily[datesSorted[j]].pageviews;
          }

          if (previousSum7 > 0) {
            metrics.daily[date].growth_7 = (sum7 / previousSum7 - 1) * 100;
          }
        }
      }

      // Calculate 28-day rolling if we have enough data
      if (i >= 27) {
        let sum28 = 0;
        for (let j = i - 27; j <= i; j++) {
          sum28 += metrics.daily[datesSorted[j]].pageviews;
        }
        metrics.daily[date].rolling_28 = sum28;

        // Calculate 28-day growth if we have enough data
        if (i >= 55) {
          let previousSum28 = 0;
          for (let j = i - 55; j <= i - 28; j++) {
            previousSum28 += metrics.daily[datesSorted[j]].pageviews;
          }

          if (previousSum28 > 0) {
            metrics.daily[date].growth_28 = (sum28 / previousSum28 - 1) * 100;
          }
        }
      }
    }
  }

  // Show error message
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
    this.updateCharts(data, yesterdayStr);
  }

  updateMetricCards(data, yesterdayStr) {
    // Get yesterday's metrics
    const yesterdayMetrics = data.daily[yesterdayStr] || {
      pageviews: 0,
      rolling_7: 0,
      rolling_28: 0,
      growth_7: 0,
      growth_28: 0,
    };

    // Format growth values with + for positive growth
    const formatGrowth = (value) => {
      const prefix = value > 0 ? "+" : "";
      return `${prefix}${value.toFixed(1)}%`;
    };

    // Color classes based on growth
    const growthColorClass = (value) => {
      if (value > 5) return "text-green-600";
      if (value < -5) return "text-red-600";
      return "text-gray-800";
    };

    document.getElementById("pageviews").innerHTML = `
      <h3 class="text-lg font-semibold mb-2">Daily Pageviews</h3>
      <p class="text-3xl">${yesterdayMetrics.pageviews.toLocaleString()}</p>
    `;

    document.getElementById("visitors").innerHTML = `
      <h3 class="text-lg font-semibold mb-2">Rolling 7-day</h3>
      <p class="text-3xl">${yesterdayMetrics.rolling_7.toLocaleString()}</p>
      <p class="text-xl ${growthColorClass(
        yesterdayMetrics.growth_7
      )}">${formatGrowth(yesterdayMetrics.growth_7)}</p>
    `;

    document.getElementById("timeOnSite").innerHTML = `
      <h3 class="text-lg font-semibold mb-2">Rolling 28-day</h3>
      <p class="text-3xl">${yesterdayMetrics.rolling_28.toLocaleString()}</p>
      <p class="text-xl ${growthColorClass(
        yesterdayMetrics.growth_28
      )}">${formatGrowth(yesterdayMetrics.growth_28)}</p>
    `;
  }

  async updateCharts(data, yesterdayStr) {
    console.log("Starting chart update with data:", data);
    console.log("Yesterday is:", yesterdayStr);

    // Verify Plotly is available
    if (!window.Plotly) {
      console.error("Plotly is not available in the global scope!");
      return;
    }

    // Filter and sort dates up to yesterday
    const dates = Object.keys(data.daily)
      .filter((date) => date <= yesterdayStr)
      .sort();

    console.log(`Found ${dates.length} dates to plot`);

    if (dates.length === 0) {
      console.error("No dates available for plotting");
      return;
    }

    // Extract data series with fallbacks
    const pageviews = dates.map((date) => data.daily[date].pageviews || 0);
    const rolling7day = dates.map((date) => data.daily[date].rolling_7 || 0);
    const rolling28day = dates.map((date) => data.daily[date].rolling_28 || 0);
    const growth7day = dates.map((date) => data.daily[date].growth_7 || 0);
    const growth28day = dates.map((date) => data.daily[date].growth_28 || 0);

    console.log("Latest pageviews:", pageviews.slice(-5));
    console.log("Latest 7-day rolling:", rolling7day.slice(-5));

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

    // Try to render each chart with error handling
    try {
      // Daily Pageviews Chart
      window.Plotly.newPlot(
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
      console.log("Daily pageviews chart rendered");

      // Rolling 7-day Chart
      window.Plotly.newPlot(
        "uniqueVisitorsChart",
        [
          {
            x: dates,
            y: rolling7day,
            type: "scatter",
            mode: "lines+markers",
            name: "7-day Rolling",
            line: { shape: "linear" },
          },
        ],
        {
          ...layout,
          title: {
            text: "7-day Rolling Pageviews",
            y: 0.95,
          },
        },
        config
      );
      console.log("7-day rolling chart rendered");

      // Rolling 28-day Chart
      window.Plotly.newPlot(
        "avgTimePerPageviewChart",
        [
          {
            x: dates,
            y: rolling28day,
            type: "scatter",
            mode: "lines+markers",
            name: "28-day Rolling",
            line: { shape: "linear" },
          },
        ],
        {
          ...layout,
          title: {
            text: "28-day Rolling Pageviews",
            y: 0.95,
          },
        },
        config
      );
      console.log("28-day rolling chart rendered");

      // Growth 7-day Chart
      window.Plotly.newPlot(
        "avgTimePerVisitorChart",
        [
          {
            x: dates,
            y: growth7day,
            type: "bar",
            name: "7-day Growth %",
            marker: {
              color: growth7day.map((val) =>
                val > 0 ? "rgba(0, 128, 0, 0.7)" : "rgba(255, 0, 0, 0.7)"
              ),
            },
          },
        ],
        {
          ...layout,
          title: {
            text: "7-day Growth Rate (%)",
            y: 0.95,
          },
        },
        config
      );
      console.log("7-day growth chart rendered");

      // Growth 28-day Chart
      window.Plotly.newPlot(
        "growthChart",
        [
          {
            x: dates,
            y: growth28day,
            type: "bar",
            name: "28-day Growth %",
            marker: {
              color: growth28day.map((val) =>
                val > 0 ? "rgba(0, 128, 0, 0.7)" : "rgba(255, 0, 0, 0.7)"
              ),
            },
          },
        ],
        {
          ...layout,
          title: {
            text: "28-day Growth Rate (%)",
            y: 0.95,
          },
        },
        config
      );
      console.log("28-day growth chart rendered");
    } catch (err) {
      console.error("Error rendering charts:", err);
    }
  }
}

// Initialize the dashboard when the script loads
console.log("Dashboard script loaded");
new DashboardUI();
