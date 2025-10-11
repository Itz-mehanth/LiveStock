import { Component, OnInit, OnDestroy, ViewChild } from '@angular/core';
import { DataService } from '../services/data.service';
import { ChartOptions, ChartData } from 'chart.js';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatSelectModule } from '@angular/material/select';
import { MatFormFieldModule } from '@angular/material/form-field';
import { HttpClientModule } from '@angular/common/http';
import { NgChartsModule, BaseChartDirective } from 'ng2-charts';
import { Subject, takeUntil } from 'rxjs';
import zoomPlugin from 'chartjs-plugin-zoom';
import { Chart } from 'chart.js';

Chart.register(zoomPlugin);

interface StockData {
  window_end: string;
  avg_price: number;
  symbol?: string;
  company?: string;
}

interface CompanyStats {
  symbol: string;
  name: string;
  currentPrice: number;
  change: number;
  changePercent: number;
  dataPoints: number;
}

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule,
    HttpClientModule,
    MatCardModule,
    MatProgressSpinnerModule,
    MatIconModule,
    MatButtonModule,
    MatTooltipModule,
    MatSelectModule,
    MatFormFieldModule,
    NgChartsModule
  ],
  templateUrl: './dashboard.html',
  styleUrls: ['./dashboard.scss'],
  providers: [DataService]
})


export class DashboardComponent implements OnInit, OnDestroy {
  @ViewChild(BaseChartDirective) chart?: BaseChartDirective;
  
  allData: StockData[] = [];
  filteredData: StockData[] = [];
  companies: CompanyStats[] = [];
  selectedCompany: string = 'ALL';
  error: string | null = null;
  isLoading = true;
  lastUpdate: Date | null = null;
  predictions: { timestamp: string; predicted_price: number }[] = [];
  selectedHorizon: string = '1_day';
  isPredicting = false;

  private destroy$ = new Subject<void>();
  private refreshInterval: any;

  // Horizon options
  horizonOptions = [
    { label: '1 Hour', value: '1_hour' },
    { label: '6 Hours', value: '6_hours' },
    { label: '12 Hours', value: '12_hours' },
    { label: '1 Day', value: '1_day' },
    { label: '3 Days', value: '3_days' },
    { label: '1 Week', value: '1_week' },
    { label: '1 Month', value: '1_month' },
    { label: '3 Months', value: '3_months' },
    { label: '6 Months', value: '6_months' },
    { label: '1 Year', value: '1_year' }
  ];

  public lineChartData: ChartData<'line'> = {
    labels: [],
    datasets: []
  };

  public lineChartOptions: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 500,
      easing: 'linear',
    },
    interaction: {
      mode: 'nearest',
      axis: 'x',
      intersect: false,
    },
    plugins: {
      legend: {
        display: true,
        position: 'top',
        labels: {
          color: '#374151',
          font: {
            size: 12,
            family: "'Inter', sans-serif",
          },
          padding: 15,
          usePointStyle: true,
          pointStyle: 'circle',
        },
      },
      tooltip: {
        enabled: true,
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        padding: 12,
        titleColor: '#fff',
        bodyColor: '#fff',
        borderColor: 'rgba(99, 102, 241, 0.5)',
        borderWidth: 1,
        displayColors: true,
        callbacks: {
          title: (items) => items[0].label,
          label: (context) => {
            const label = context.dataset.label || '';
            return `${label}: $${context.parsed.y.toFixed(2)}`;
          },
        },
      },
      zoom: {
        zoom: {
          wheel: { enabled: true, speed: 0.1 },
          pinch: { enabled: true },
          mode: 'x',
        },
        pan: {
          enabled: true,
          mode: 'x',
        },
        limits: {
          x: { min: 'original', max: 'original' },
        },
      },
    },
    scales: {
      x: {
        display: true,
        grid: {
          display: true,
          color: 'rgba(0, 0, 0, 0.05)',
          drawTicks: false,
        },
        ticks: {
          color: '#6b7280',
          maxRotation: 0,
          autoSkipPadding: 20,
          font: { size: 11, family: "'Inter', sans-serif" },
        },
        border: { display: false },
        min: undefined, // dynamically set to last 100 points
        max: undefined, // dynamically set to last 100 points
      },
      y: {
        display: true,
        position: 'right',
        grid: {
          color: 'rgba(0, 0, 0, 0.05)',
          drawTicks: false,
        },
        ticks: {
          color: '#6b7280',
          callback: (value) => '$' + Number(value).toFixed(2),
          font: { size: 11, family: "'Inter', sans-serif" },
          padding: 8,
        },
        border: { display: false },
      },
    },
  };

  // Call this method to fetch predictions
  fetchPredictions(): void {
    if (!this.selectedCompany || this.selectedCompany === 'ALL') return;

    this.isPredicting = true;
    this.predictions = [];

    this.dataService.getFutureTrends(this.selectedCompany, this.selectedHorizon)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (res: any) => {
          this.predictions = res.predictions || [];
          this.isPredicting = false;
        },
        error: (err) => {
          console.error('Error fetching predictions', err);
          this.isPredicting = false;
        }
      });
  }

  constructor(private dataService: DataService) {}

  ngOnInit(): void {
    this.fetchData();
    this.refreshInterval = setInterval(() => this.fetchData(), 5000);
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
    }
  }

  fetchData(): void {
    this.dataService.getProcessedData()
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: (res: StockData[]) => {
          this.allData = res;
          this.error = null;
          this.isLoading = false;
          this.lastUpdate = new Date();
          this.processCompanies(res);
          this.filterDataByCompany(this.selectedCompany);
        },
        error: (err) => {
          console.error('Error fetching data', err);
          this.error = err.message || 'Unable to fetch data';
          this.isLoading = false;
        },
      });
  }

  processCompanies(data: StockData[]): void {
    const companyMap = new Map<string, StockData[]>();
    
    // Group data by company/symbol
    data.forEach(item => {
      const symbol = item.symbol || item.company || 'UNKNOWN';
      if (!companyMap.has(symbol)) {
        companyMap.set(symbol, []);
      }
      companyMap.get(symbol)!.push(item);
    });

    // Calculate stats for each company
    this.companies = Array.from(companyMap.entries()).map(([symbol, items]) => {
      const sortedItems = [...items].sort((a, b) => 
        new Date(a.window_end).getTime() - new Date(b.window_end).getTime()
      );
      
      const currentPrice = sortedItems[sortedItems.length - 1]?.avg_price || 0;
      const previousPrice = sortedItems[sortedItems.length - 2]?.avg_price || currentPrice;
      const change = currentPrice - previousPrice;
      const changePercent = previousPrice !== 0 ? (change / previousPrice) * 100 : 0;

      return {
        symbol,
        name: symbol,
        currentPrice,
        change,
        changePercent,
        dataPoints: items.length
      };
    });

    // Sort companies by symbol
    this.companies.sort((a, b) => a.symbol.localeCompare(b.symbol));
  }

  filterDataByCompany(symbol: string): void {
    this.selectedCompany = symbol;
    
    if (symbol === 'ALL') {
      this.filteredData = this.allData;
      this.updateChartMultipleCompanies();
    } else {
      this.filteredData = this.allData.filter(item => 
        (item.symbol || item.company || 'UNKNOWN') === symbol
      );
      this.updateChartSingleCompany();
    }
  }

  updateChartSingleCompany(): void {
    const sortedData = [...this.filteredData].sort((a, b) => 
      new Date(a.window_end).getTime() - new Date(b.window_end).getTime()
    );

    this.lineChartData.labels = sortedData.map(d => 
      new Date(d.window_end).toLocaleString('en-US', { 
        month: 'short',
        day: 'numeric',
        hour: '2-digit', 
        minute: '2-digit'
      })
    );

    this.lineChartData.datasets = [{
      data: sortedData.map(d => d.avg_price),
      label: this.selectedCompany,
      fill: true,
      tension: 0.4,
      borderColor: 'rgb(99, 102, 241)',
      backgroundColor: 'rgba(99, 102, 241, 0.1)',
      pointRadius: 0,
      pointHoverRadius: 6,
      pointHoverBackgroundColor: 'rgb(99, 102, 241)',
      pointHoverBorderColor: '#fff',
      pointHoverBorderWidth: 2,
      borderWidth: 2,
    }];

    // --- Auto-scroll to last 100 points ---
    const last100 = sortedData.slice(-100);
    if (last100.length > 0) {
      this.lineChartOptions.scales!['x']!.min = last100[0].window_end;
      this.lineChartOptions.scales!['x']!.max = last100[last100.length - 1].window_end;
    }

    this.chart?.update('none');
  }

  updateChartMultipleCompanies(): void {
    const colors = [
      { border: 'rgb(99, 102, 241)', bg: 'rgba(99, 102, 241, 0.1)' },
      { border: 'rgb(168, 85, 247)', bg: 'rgba(168, 85, 247, 0.1)' },
      { border: 'rgb(59, 130, 246)', bg: 'rgba(59, 130, 246, 0.1)' },
      { border: 'rgb(16, 185, 129)', bg: 'rgba(16, 185, 129, 0.1)' },
      { border: 'rgb(245, 158, 11)', bg: 'rgba(245, 158, 11, 0.1)' },
      { border: 'rgb(239, 68, 68)', bg: 'rgba(239, 68, 68, 0.1)' },
    ];

    const companyData = new Map<string, StockData[]>();
    this.allData.forEach(item => {
      const symbol = item.symbol || item.company || 'UNKNOWN';
      if (!companyData.has(symbol)) {
        companyData.set(symbol, []);
      }
      companyData.get(symbol)!.push(item);
    });

    // Get all unique timestamps
    const allTimestamps = new Set<string>();
    this.allData.forEach(item => allTimestamps.add(item.window_end));
    const sortedTimestamps = Array.from(allTimestamps).sort();

    this.lineChartData.labels = sortedTimestamps.map(ts => 
      new Date(ts).toLocaleString('en-US', { 
        month: 'short',
        day: 'numeric',
        hour: '2-digit', 
        minute: '2-digit'
      })
    );

    this.lineChartData.datasets = Array.from(companyData.entries()).map(([symbol, data], index) => {
      const sortedData = [...data].sort((a, b) => 
        new Date(a.window_end).getTime() - new Date(b.window_end).getTime()
      );

      const color = colors[index % colors.length];

      return {
        data: sortedData.map(d => d.avg_price),
        label: symbol,
        fill: false,
        tension: 0.4,
        borderColor: color.border,
        backgroundColor: color.bg,
        pointRadius: 0,
        pointHoverRadius: 6,
        pointHoverBackgroundColor: color.border,
        pointHoverBorderColor: '#fff',
        pointHoverBorderWidth: 2,
        borderWidth: 2,
      };
    });

    const last100Timestamps = sortedTimestamps.slice(-100);
    if (last100Timestamps.length > 0) {
      this.lineChartOptions.scales!['x']!.min = last100Timestamps[0];
      this.lineChartOptions.scales!['x']!.max = last100Timestamps[last100Timestamps.length - 1];
    }


    this.chart?.update('none');

  }

  resetZoom(): void {
    this.chart?.chart?.resetZoom();
  }

  getSelectedCompanyStats(): CompanyStats | null {
    if (this.selectedCompany === 'ALL') return null;
    return this.companies.find(c => c.symbol === this.selectedCompany) || null;
  }

  get isPriceUp(): boolean {
    const stats = this.getSelectedCompanyStats();
    return stats ? stats.change > 0 : false;
  }

  get isPriceDown(): boolean {
    const stats = this.getSelectedCompanyStats();
    return stats ? stats.change < 0 : false;
  }

  get currentPrice(): number {
    const stats = this.getSelectedCompanyStats();
    return stats?.currentPrice || 0;
  }

  get priceChange(): number {
    const stats = this.getSelectedCompanyStats();
    return stats?.change || 0;
  }

  get priceChangePercent(): number {
    const stats = this.getSelectedCompanyStats();
    return stats?.changePercent || 0;
  }

  get totalDataPoints(): number {
    return this.selectedCompany === 'ALL' 
      ? this.allData.length 
      : this.filteredData.length;
  }
}
