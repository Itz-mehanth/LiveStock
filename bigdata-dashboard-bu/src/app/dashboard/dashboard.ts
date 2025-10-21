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
import 'chartjs-adapter-date-fns';

Chart.register(zoomPlugin);

// Interfaces remain the same
interface StockData { window_end: string; avg_price: number; symbol?: string; }
interface CompanyStats { symbol: string; name: string; currentPrice: number; change: number; changePercent: number; dataPoints: number; }

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule, HttpClientModule, MatCardModule, MatProgressSpinnerModule,
    MatIconModule, MatButtonModule, MatTooltipModule, MatSelectModule,
    MatFormFieldModule, NgChartsModule
  ],
  templateUrl: './dashboard.html',
  styleUrls: ['./dashboard.scss'],
  providers: [DataService]
})
export class DashboardComponent implements OnInit, OnDestroy {
  @ViewChild(BaseChartDirective) chart?: BaseChartDirective;
  
  readonly allowedSymbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META"];
  allData: StockData[] = [];
  companies: CompanyStats[] = [];
  selectedCompany: string = 'ALL';
  error: string | null = null;
  isLoading = true;
  lastUpdate: Date | null = null;
  predictions: { timestamp: string; predicted_price: number }[] = [];
  selectedHorizon: string = '1_week';
  isPredicting = false;
  private destroy$ = new Subject<void>();
  private refreshInterval: any;

  horizonOptions = [
    { label: '1 Day', value: '1_day' }, { label: '3 Days', value: '3_days' },
    { label: '1 Week', value: '1_week' }, { label: '1 Month', value: '1_month' }
  ];

  public lineChartData: ChartData<'line'> = { datasets: [] };
  public lineChartOptions: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', axis: 'x', intersect: false },
    plugins: {
      legend: { position: 'top', labels: { usePointStyle: true, pointStyle: 'circle', padding: 20 }},
      tooltip: {
        position: 'nearest',
        callbacks: {
          label: (context) => `${context.dataset.label || ''}: $${(context.raw as any).y.toFixed(2)}`
        }
      },
      zoom: {
        pan: { enabled: true, mode: 'x' },
        zoom: { wheel: { enabled: true, speed: 0.1 }, pinch: { enabled: true }, mode: 'x' }
      },
    },
    scales: {
      x: {
        type: 'time',
        time: {
          tooltipFormat: 'PP pp',
          displayFormats: { hour: 'p', minute: 'p' }
        },
        ticks: { source: 'auto', maxRotation: 0, autoSkip: true },
        grid: { drawOnChartArea: false }
      },
      y: {
        position: 'right',
        ticks: { callback: (value) => '$' + Number(value).toFixed(2) },
        grid: { color: '#e2e8f0' }
      },
    },
  };

  constructor(private dataService: DataService) {}

  ngOnInit(): void {
    this.fetchData();
    this.refreshInterval = setInterval(() => this.fetchData(), 5000);
  }

  ngOnDestroy(): void {
    this.destroy$.next(); this.destroy$.complete();
    if (this.refreshInterval) clearInterval(this.refreshInterval);
  }

  fetchData(): void {
    this.dataService.getProcessedData().pipe(takeUntil(this.destroy$)).subscribe({
      next: (res) => {
        const filteredRes = res.filter(item => this.allowedSymbols.includes(item.symbol || ''));
        this.allData = filteredRes;
        this.error = null;
        this.isLoading = false;
        this.lastUpdate = new Date();
        this.processCompanies(this.allData);
        this.filterDataByCompany(this.selectedCompany);
      },
      error: (err) => { this.error = err.message || 'Unable to fetch data'; this.isLoading = false; },
    });
  }
  
  fetchPredictions(): void {
    if (!this.selectedCompany || this.selectedCompany === 'ALL') return;
    this.isPredicting = true;
    this.predictions = [];
    this.dataService.getFutureTrends(this.selectedCompany, this.selectedHorizon)
      .pipe(takeUntil(this.destroy$)).subscribe({
        next: (res) => { this.predictions = res.predictions || []; this.isPredicting = false; },
        error: (err) => { console.error(err); this.isPredicting = false; }
      });
  }

  processCompanies(data: StockData[]): void {
    const companyMap = new Map<string, StockData[]>();
    data.forEach(item => {
      const symbol = item.symbol || 'UNKNOWN';
      if (!companyMap.has(symbol)) companyMap.set(symbol, []);
      companyMap.get(symbol)!.push(item);
    });
    this.companies = Array.from(companyMap.entries()).map(([symbol, items]) => {
      const sorted = [...items].sort((a, b) => new Date(a.window_end).getTime() - new Date(b.window_end).getTime());
      const current = sorted[sorted.length - 1];
      const previous = sorted[sorted.length - 2] || current;
      const change = (current?.avg_price || 0) - (previous?.avg_price || 0);
      const changePercent = previous?.avg_price ? (change / previous.avg_price) * 100 : 0;
      return { symbol, name: symbol, currentPrice: current?.avg_price || 0, change, changePercent, dataPoints: items.length };
    });
    this.companies.sort((a, b) => a.symbol.localeCompare(b.symbol));
  }

  filterDataByCompany(symbol: string): void {
    this.selectedCompany = symbol;
    if (symbol === 'ALL') this.updateChartMultipleCompanies();
    else this.updateChartSingleCompany();
  }

  updateChartSingleCompany(): void {
    const companyData = this.allData.filter(item => item.symbol === this.selectedCompany);
    this.lineChartData.datasets = [{
      data: companyData.map(d => ({ x: new Date(d.window_end).valueOf(), y: d.avg_price })),
      label: this.selectedCompany,
      fill: true,
      tension: 0, // --- THIS IS THE CHANGE: Straight lines ---
      borderColor: 'rgb(79, 70, 229)',
      backgroundColor: 'rgba(99, 102, 241, 0.1)',
      pointBackgroundColor: 'rgb(79, 70, 229)',
      pointHoverBackgroundColor: 'rgb(79, 70, 229)',
      pointHoverBorderColor: '#fff',
    }];
    this.chart?.update('none');
  }

  updateChartMultipleCompanies(): void {
    const colors = [
      { color: 'rgb(239, 68, 68)' }, { color: 'rgb(59, 130, 246)' },
      { color: 'rgb(245, 158, 11)' }, { color: 'rgb(16, 185, 129)' },
      { color: 'rgb(139, 92, 246)' }, { color: 'rgb(217, 70, 239)' },
      { color: 'rgb(96, 165, 250)' }
    ];
    this.lineChartData.datasets = this.allowedSymbols.map((symbol, index) => {
      const companyData = this.allData.filter(item => item.symbol === symbol);
      const color = colors[index % colors.length].color;
      return {
        data: companyData.map(d => ({ x: new Date(d.window_end).valueOf(), y: d.avg_price })),
        label: symbol,
        fill: false,
        tension: 0, // --- THIS IS THE CHANGE: Straight lines ---
        borderColor: color,
        backgroundColor: color,
        pointBackgroundColor: color,
        pointHoverBackgroundColor: color,
        pointHoverBorderColor: '#fff',
      };
    });
    this.chart?.update('none');
  }

  resetZoom(): void { this.chart?.chart?.resetZoom(); }

  get stats() { return this.companies.find(c => c.symbol === this.selectedCompany) || null; }
  get isPriceUp() { return this.stats ? this.stats.change > 0 : false; }
  get isPriceDown() { return this.stats ? this.stats.change < 0 : false; }
  get currentPrice() { return this.stats?.currentPrice || 0; }
  get priceChange() { return this.stats?.change || 0; }
  get priceChangePercent() { return this.stats?.changePercent || 0; }
  get totalDataPoints() { return this.allData.length; }
}

